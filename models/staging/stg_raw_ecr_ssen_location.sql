WITH stg_raw_ecr_ssen_location AS (
    SELECT
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        poc_voltage,
        licence_area,
        chp_cogeneration_1,
        registered_cap_1,
        already_connected_cap_mw,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        -- Create geopoint using NULL for non-numeric values
        ST_SetSRID(ST_MakePoint(
            NULLIF(x_coordinate, 'REDACTED')::float,
            NULLIF(y_coordinate, 'REDACTED')::float
        ), 27700) AS geopoint
    FROM {{ ref('stg_raw_ecr_ssen') }}
),

-- Join to primary and BSP tables using spatial joins within EPSG:27700
location_with_geo_data AS (
    SELECT
        loc.*,
        geo_primary.primary AS derived_pry_group,
        geo_sepd_bsp.bsp_name AS derived_bsp_group,
        geo_sepd_bsp.bsp_kv AS derived_bsp_kv,
        geo_sepd_bsp.bsp_alias AS derived_bsp_alias,
        geo_sepd_gsp.gsp_name AS derived_sepd_gsp_group,
        geo_shepd_gsp.gsp_name AS derived_shepd_gsp_group,  -- Distinct alias for each source
        geo_sepd_gsp.gsp_alias AS derived_gsp_alias,
        geo_sepd_gsp.licence_ar AS derived_licence_ar,
        geo_shepd_gsp.region AS derived_region,
        coalesce(loc.primary_boundary, geo_primary.primary) as primary_boundary_used,
        coalesce(loc.already_connected_cap_mw, loc.registered_cap_1) as capacity_used
    FROM
        stg_raw_ecr_ssen_location AS loc
    LEFT JOIN
        public.geo_ssen_primary AS geo_primary
    ON
        ST_Within(loc.geopoint, geo_primary.wkb_geometry)
    LEFT JOIN
        public.geo_ssen_sepd_bsp AS geo_sepd_bsp
    ON
        ST_Within(loc.geopoint, geo_sepd_bsp.geom)
    LEFT JOIN
        public.geo_ssen_sepd_gsp AS geo_sepd_gsp
    ON
        ST_Within(loc.geopoint, geo_sepd_gsp.geom)
    LEFT JOIN
        public.geo_ssen_shepd_gsp AS geo_shepd_gsp
    ON
        ST_Within(loc.geopoint, geo_shepd_gsp.geom)
)

SELECT * FROM location_with_geo_data
