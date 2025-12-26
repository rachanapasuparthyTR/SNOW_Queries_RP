WITH		
parents as (		
   SELECT ID AS AGENT_PARENTID, CUSTOM_CUSTOMERNAME, CUSTOM_SRC_CUST_ID,
   --custom_edl2p_gsi_name, 
   AGENT_INSTANCECATEGORY, AGENT_INSTANCETYPE ,  
   -- custom_edl_2_p_gsi_bu_segment_level_2_c,        		
    CUSTOM_TR_MARKET_SEGMENT_L_1,CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY, custom_totalsize, custom_currentstoragelimit, custom_externaluserlimit, custom_internaluserlimit		
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    -- AND CUSTOM_TR_MARKET_SEGMENT_L_1 LIKE  'Corporate'		
    AND AGENT_INSTANCECATEGORY LIKE 'Client' AND AGENT_INSTANCETYPE LIKE 'Live'		
    -- AND CUSTOM_COUNTRY like 'CA'
    -- AND custom_edl_2_p_gsi_bu_segment_level_2_c is not null
    QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
    
)
,ACCOUNTS_1 AS (		 
    SELECT ID AS ACCOUNTID,
    AGENT_PARENTID, CUSTOM_SRC_CUST_ID,AGENT_SITEPURPOSE ,AGENT_ISSITETEMPLATE,LAST_UPDATED_AT		
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    --AND CONTAINS(lower(AGENT_SITEPURPOSE),'client portal')		
    AND AGENT_PARENTID IN (SELECT AGENT_PARENTID FROM parents) 		
    QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
),

ALL_ACCOUNTS AS (		
SELECT A.ACCOUNTID, B.AGENT_PARENTID,A.CUSTOM_SRC_CUST_ID, A.AGENT_SITEPURPOSE ,A.AGENT_ISSITETEMPLATE, B.CUSTOM_CUSTOMERNAME,  B.AGENT_INSTANCECATEGORY, B.AGENT_INSTANCETYPE ,          		
B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY	,custom_totalsize, custom_currentstoragelimit, custom_externaluserlimit, custom_internaluserlimit	
FROM ACCOUNTS_1 A LEFT JOIN parents B ON 		
A.AGENT_PARENTID= B.AGENT_PARENTID		

) 

, 		
FEATURE_EVENTS AS (		
    SELECT		
    cast(TIMESTAMP AS DATE) AS DDMMYYYY,		
    --CONCAT(day(TIMESTAMP),':',MONTH(TIMESTAMP),':', YEAR(TIMESTAMP)) AS DDMMYYYY,    		
    account_id, 		
    visitor_id, 		
    FEATURE_ID, 		
    SUM(num_events) as feature_clicks		
	FROM	prod.source.pendo_highq_feature_event_vw
	WHERE	APP_ID LIKE '-323232' AND to_date(convert_timezone('UTC','America/New_York',timestamp::timestamp_ntz)) >= '2025-09-30'    
    AND to_date(convert_timezone('UTC','America/New_York',timestamp::timestamp_ntz)) <= '2025-11-30'
    GROUP BY 1,2,3,4		
   
   )		
		
, feature_id as ( 		
    SELECT	id,name,group_id	
	FROM prod.source.pendo_highq_feature_history_vw	
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
    )		
		
    ,		
product_area as(		
    select ID as group_id, Name from PROD.SOURCE.PENDO_HIGHQ_GROUP_VW		 
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
   
)		
, FEATURE_GROUPING AS(

    SELECT DDMMYYYY, ACCOUNT_ID, VISITOR_ID, FEATURE_ID, F.NAME AS FEATURE_NAME,
     P.NAME AS PRODUCT_AREA, feature_clicks

    FROM    FEATURE_EVENTS A LEFT JOIN FEATURE_ID F ON A.FEATURE_ID=F.ID
    LEFT JOIN PRODUCT_AREA P ON F.GROUP_ID = P.GROUP_ID

    WHERE FEATURE_NAME NOT LIKE '%DO NOT USE%' 
    AND FEATURE_NAME NOT LIKE '%- to review'
    
)
, FEATURE_PRODUCTAREA_ALL AS(
select   B.CUSTOM_CUSTOMERNAME, B.CUSTOM_SRC_CUST_ID,CUSTOM_TR_MARKET_SEGMENT_L_1,
CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY,
 custom_totalsize, custom_currentstoragelimit, custom_externaluserlimit, custom_internaluserlimit,	
 COUNT(DISTINCT B.AGENT_PARENTID) AS ACCOUNTS,
count(distinct ACCOUNT_ID) as SITES,
--B.AGENT_SITEPURPOSE,
 --MONTH(F.DDMMYYYY), 
-- F.PRODUCT_AREA AS F_PRODUCT_AREA,
 --COUNT(DISTINCT F.ACCOUNT_ID) AS F_SITES,
SUM(F.feature_clicks) AS CLICKS,
COUNT(DISTINCT F.visitor_id) AS Users_U
FROM FEATURE_GROUPING F
RIGHT JOIN ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
where F.FEATURE_NAME is not null		
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY CLICKS DESC
)

select * from FEATURE_PRODUCTAREA_ALL