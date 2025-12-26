-- ROLE: A208177_PRODUCT_ANALYTICS_DATA_MDS_OWNER



-- Main query with averages, 75th percentiles, and counts
SELECT 
   view_name,  
   -- Loading Time metrics
   ROUND(AVG(CASE WHEN view_loading_time_ns IS NOT NULL 
             THEN view_loading_time_ns::FLOAT / 1e9 END), 2) AS loading_time_avg,
   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_loading_time_ns::FLOAT / 1e9), 2) AS loading_time_p75,
   COUNT(view_loading_time_ns) AS loading_time_count,
  
   -- LCP metrics
   ROUND(AVG(CASE WHEN view_lcp_ns IS NOT NULL
             THEN view_lcp_ns::FLOAT / 1e9 END), 2) AS lcp_avg,
   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_lcp_ns::FLOAT / 1e9), 2) AS lcp_p75,
   COUNT(view_lcp_ns) AS lcp_count,
  
   -- FCP metrics
   ROUND(AVG(CASE WHEN view_fcp_ns IS NOT NULL
             THEN view_fcp_ns::FLOAT / 1e9 END), 2) AS fcp_avg,
   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_fcp_ns::FLOAT / 1e9), 2) AS fcp_p75,
   COUNT(view_fcp_ns) AS fcp_count,
  
   -- CLS metrics
   ROUND(AVG(CASE WHEN view_cls IS NOT NULL 
             THEN view_cls::FLOAT END), 4) AS cls_avg,
   ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_cls::FLOAT), 4) AS cls_p75,
   COUNT(view_cls) AS cls_count,
  
   -- Total views
   COUNT(*) AS num_views
  
FROM FD
WHERE service = 'a208730-mfe-container-application' 
   AND app_name = 'a208730-MFE-Container-Application'
   AND session_type = 'user' 
   AND DATE(event_ts) between '2025-08-01' and '2025-08-10'
GROUP BY 1
ORDER BY 1;