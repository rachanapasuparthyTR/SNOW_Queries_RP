
--ROLE: A208177_PRODUCT_ANALYTICS_DATA_MDS_OWNER

WITH FD AS
(
SELECT
-- *,
-- Root/meta
REA_TYPE as event_type,
REA_DD_FORMAT_VERSION as dd_format_version,
 
--User/Org
REA_RESOURCE_URL_QUERY_USERID as user_id,
REA_USR_ORGANIZATIONID as org_id,
 
 
-- Application
REA_APPLICATION_ID as app_id,
REA_APPLICATION_SHORT_NAME as app_short_name,
REA_APPLICATION_NAME as app_name,
 
 
-- Browser
REA_BROWSER_NAME as browser_name,
REA_BROWSER_VERSION as browser_version,
REA_BROWSER_VERSION_MAJOR as browser_version_major,
 
-- Device
REA_DEVICE_NAME as device_name,
REA_DEVICE_TYPE as device_type,
REA_CONTEXT_DEVICE_SCREEN_RESOLUTION as device_screen_resolution,
 
-- Display
REA_DISPLAY_VIEWPORT_HEIGHT as viewport_height,
REA_DISPLAY_VIEWPORT_WIDTH as viewport_width,
 
 
 
-- Geo
REA_GEO_AS_DOMAIN as geo_as_domain,
REA_GEO_AS_NAME as geo_as_name,
REA_GEO_AS_TYPE as geo_as_type,
REA_GEO_CITY as geo_city,
REA_GEO_CONTINENT as geo_content,
REA_GEO_CONTINENT_CODE as geo_continent_code,
REA_GEO_COUNTRY as geo_country,
REA_GEO_COUNTRY_ISO_CODE as geo_country_iso_code,
REA_GEO_COUNTRY_SUBDIVISION as geo_region,
REA_GEO_COUNTRY_SUBDIVISION_ISO_CODE as geo_region_iso,
REA_GEO_LATITUDE geo_latitude,
REA_GEO_LONGITUDE as geo_longitude,
REA_GEO_LOCATION as geo_location,
 
-- OS
REA_OS_NAME as os_name,
 
 
-- Privacy
REA_PRIVACY_REPLAY_LEVEL as privacy_replay_level,
 
-- Service
REA_SERVICE as service,
 
-- Session
REA_SESSION_ID as session_id,
REA_SESSION_IP as session_ip,
REA_SESSION_PLAN as session_plan,
REA_SESSION_TYPE as session_type,
REA_SESSION_HAS_REPLAY as session_has_replay,
REA_SESSION_HAS_FULL_SNAPSHOT as session_has_full_snapshot,
REA_SESSION_USERAGENT as session_useragent,
REA_SESSION_MATCHING_RETENTION_FILTER_ID as session_mrf_id,
REA_SESSION_MATCHING_RETENTION_FILTER_NAME as session_mrf_name,
 
-- Synthetics
REA_SYNTHETICS_INJECTED as synthetics_injected,
REA_SYNTHETICS_TEST_ID as synthetics_test_id,
REA_SYNTHETICS_RESULT_ID as synthetics_result_id,
 
 
-- View summary
REA_VIEW_ID as view_id,
REA_VIEW_NAME as view_name,
REA_VIEW_URL as view_url,
REA_VIEW_URL_SCHEME as view_url_scheme,
REA_VIEW_URL_HOST as view_url_host,
REA_VIEW_URL_DOMAIN as view_url_domain,
REA_VIEW_URL_PATH as view_url_path,
REA_VIEW_URL_PATH_GROUP as view_url_path_group,
REA_VIEW_REFERRER as view_referrer,
REA_VIEW_REFERRER_URL_URL_HOST as view_referrer_host,
REA_VIEW_REFERRER_URL_URL_PATH as view_referrer_path,
REA_VIEW_REFERRER_URL_URL_PATH_GROUP as view_referrer_path_group,
REA_VIEW_REFERRER_URL_URL_SCHEME as view_referrer_scheme,
REA_VIEW_URL_QUERY_STATE as view_urlquery_state,
REA_VIEW_URL_QUERY_UI_LOCALES as view_urlquery_ui_locales,
 
 
-- View metrics and counts (nanoseconds in payload)
 
-- NOT FOUND!
-- RUM_EVENT_ATTRUBUTE:view:in_foreground_periods:duration::NUMBER AS view_fg_first_duration_ns,
-- RUM_EVENT_ATTRUBUTE:view:in_foreground_periods:start::NUMBER AS view_fg_first_start_ns,
 
REA_VIEW_ACTION_COUNT as view_action_count,
REA_VIEW_ERROR_COUNT as view_error_count,
REA_VIEW_FRUSTRATION_COUNT as view_frustration_count,
REA_VIEW_RESOURCE_COUNT as view_resource_count,
REA_VIEW_IS_ACTIVE as view_is_active,
REA_VIEW_IN_FOREGROUND_PERIODS as view_fg_first_start_ns,
 
REA_VIEW_LOADING_TYPE as view_loading_type,
REA_VIEW_CUMULATIVE_LAYOUT_SHIFT as view_cls,
REA_VIEW_FIRST_BYTE as first_byte_ns,
REA_VIEW_FIRST_CONTENTFUL_PAINT as view_fcp_ns,
REA_VIEW_LARGEST_CONTENTFUL_PAINT view_lcp_ns,
REA_VIEW_DOM_INTERACTIVE as view_dom_interactive_ns,
REA_VIEW_DOM_CONTENT_LOADED as view_dom_content_loaded_ns,
REA_VIEW_DOM_COMPLETE as view_dom_complete_ns,
REA_VIEW_LOAD_EVENT as view_load_event_ns,
REA_VIEW_LOADING_TIME as view_loading_time_ns,
REA_VIEW_TIME_SPENT as view_time_spent_ns,
 
 
-- View performance nested
REA_VIEW_PERFORMANCE_CLS_SCORE as perf_cls_score,
REA_VIEW_PERFORMANCE_FCP_TIMESTAMP as perf_fcp_timestamp_ns,
REA_VIEW_PERFORMANCE_LCP_TIMESTAMP as perf_lcp_timestamp_ns,
TIMESTAMPS as event_ts,
REA_VIEW_PERFORMANCE_FID_TIMESTAMP as fid_timestamp,
REA_VIEW_PERFORMANCE_INP_TIMESTAMP as inp_timestamp,
REA_VIEW_PERFORMANCE_CLS_TIMESTAMP as cls_timestamp,
REA_VIEW_PERFORMANCE_FCP_TIMESTAMP as fcp_timestamp,
REA_CONTEXT_HTTPERROR_ERROR_TIMESTAMP as http_error_timestamp,
REA_VIEW_PERFORMANCE_LCP_TIMESTAMP as lcp_timestamp,
REA_RESOURCE_URL_QUERY_TIMESTAMP as url_query_timestamp
 
 
FROM MYDATASPACE.A208177_PRODUCT_ANALYTICS_DATA.COCOUNSEL_DATADOG_RUM_EVENT_FLAT_prod
WHERE SERVICE = 'a208730-mfe-container-application' 
   AND APP_NAME = 'a208730-MFE-Container-Application'
   AND SESSION_TYPE = 'user' 
limit 2000
)

SELECT 

ROUND(AVG(CASE WHEN view_loading_time_ns IS NOT NULL THEN view_loading_time_ns::FLOAT / 1e9 END), 2) AS loading_time_avg,
ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_loading_time_ns::FLOAT / 1e9), 2) AS loading_time_p75,
ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY view_loading_time_ns::FLOAT / 1e9), 2) AS loading_time_p50,

ROUND(AVG(CASE WHEN view_lcp_ns IS NOT NULL THEN view_lcp_ns::FLOAT / 1e9 END), 2) AS lcp_avg,

ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_lcp_ns::FLOAT / 1e9), 2) AS lcp_p75, 

ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY view_lcp_ns::FLOAT / 1e9), 2) AS lcp_p50,

ROUND(AVG(CASE WHEN view_fcp_ns IS NOT NULL THEN view_fcp_ns::FLOAT / 1e9 END), 2) AS fcp_avg,
ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_fcp_ns::FLOAT / 1e9), 2) AS fcp_p75,
ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY view_fcp_ns::FLOAT / 1e9), 2) AS fcp_p50,
-- CLS metrics
ROUND(AVG(CASE WHEN view_cls IS NOT NULL THEN view_cls::FLOAT END), 4) AS cls_avg,
ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_cls::FLOAT), 4) AS cls_p75,
ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY view_cls::FLOAT), 4) AS cls_p50

FROM FD