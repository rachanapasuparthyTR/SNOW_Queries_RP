WITH		
parents as (		
    SELECT ID AS AGENT_PARENTID, CUSTOM_CUSTOMERNAME,  AGENT_INSTANCECATEGORY, AGENT_INSTANCETYPE ,          		
    CUSTOM_TR_MARKET_SEGMENT_L_1,CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY
    --, CUSTOM_EDL_2_P_GSI_NAME		
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    AND AGENT_INSTANCECATEGORY LIKE 'Client' AND AGENT_INSTANCETYPE LIKE 'Live'		
    QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
),		
		
ACCOUNTS_1 AS (		
    SELECT ID AS ACCOUNTID, AGENT_PARENTID, AGENT_SITEPURPOSE ,AGENT_ISSITETEMPLATE,LAST_UPDATED_AT		
		
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    --AND CONTAINS(lower(AGENT_SITEPURPOSE),'client portal')		
    --AND AGENT_SITEPURPOSE LIKE 'Contract Management'		
    AND AGENT_PARENTID IN (SELECT AGENT_PARENTID FROM parents) 		
    QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
), 		
ALL_ACCOUNTS AS (		
SELECT A.ACCOUNTID, B.AGENT_PARENTID, A.AGENT_SITEPURPOSE ,A.AGENT_ISSITETEMPLATE, B.CUSTOM_CUSTOMERNAME,  B.AGENT_INSTANCECATEGORY, B.AGENT_INSTANCETYPE ,          		
B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY		
FROM ACCOUNTS_1 A LEFT JOIN parents B ON 		
A.AGENT_PARENTID= B.AGENT_PARENTID		
WHERE UPPER(AGENT_SITEPURPOSE) LIKE '%DUE DILIGENCE%'

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
	WHERE	APP_ID LIKE '-323232' AND to_date(convert_timezone('UTC','America/New_York',timestamp::timestamp_ntz)) BETWEEN '2025-06-01' AND '2025-08-31'    
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

    SELECT DDMMYYYY, ACCOUNT_ID, VISITOR_ID, FEATURE_ID, F.NAME AS FEATURE_NAME, P.NAME AS PRODUCT_AREA, feature_clicks

    FROM    FEATURE_EVENTS A LEFT JOIN FEATURE_ID F ON A.FEATURE_ID=F.ID
    LEFT JOIN PRODUCT_AREA P ON F.GROUP_ID = P.GROUP_ID

    WHERE FEATURE_NAME NOT LIKE '%DO NOT USE%' 
    AND FEATURE_NAME NOT LIKE '%- to review'
    and PRODUCT_AREA IS NOT NULL
)
----------------------------------------------------------------------------------------------------------------------------------------------------
-- below query is for Actuve Visitors
----------------------------------------------------------------------------------------------------------------------------------------------------
-- SELECT ACCOUNT_ID, B.AGENT_PARENTID AS PARENT_ID, B.AGENT_SITEPURPOSE , B.CUSTOM_CUSTOMERNAME
-- ,B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY ,		
-- PRODUCT_AREA, FEATURE_NAME, SUM(FEATURE_CLICKS) AS FEATURECLICKS, COUNT(DISTINCT VISITOR_ID)
-- FROM FEATURE_GROUPING F
-- RIGHT JOIN ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
-- where F.FEATURE_NAME is not null		
-- GROUP BY 1,2,3,4,5,6,7,8,9,10
-- ORDER BY FEATURECLICKS DESC


----------------------------------------------------------------------------------------------------------------------------------------------------
-- below query is for monthly features usage
----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT MONTH(DDMMYYYY) AS MM, ACCOUNT_ID, B.AGENT_PARENTID AS PARENT_ID, B.AGENT_SITEPURPOSE , B.CUSTOM_CUSTOMERNAME
,B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY ,		
PRODUCT_AREA, FEATURE_NAME, SUM(FEATURE_CLICKS) AS FEATURECLICKS, COUNT(DISTINCT VISITOR_ID)
FROM FEATURE_GROUPING F
RIGHT JOIN ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
where F.FEATURE_NAME is not null		
AND upper(PRODUCT_AREA) LIKE ANY ('SITEWIDE','FILES','FILES - FILE VIEWER', 'SITE ADMIN','ISHEETS','HOME') 
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY FEATURECLICKS DESC
----------------------------------------------------------------------------------------------------------------------------------------------------



-- SELECT PRODUCT_AREA, COUNT(DISTINCT FEATURE_NAME)
-- FROM FEATURE_GROUPING F
-- RIGHT JOIN ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
-- where F.FEATURE_NAME is not null
-- GROUP BY 1
----------------------------------------------------------------------------------------------------------------------------------------------------
-- below query is for monthly modules usage
----------------------------------------------------------------------------------------------------------------------------------------------------

-- SELECT MONTH(DDMMYYYY) AS MM, ACCOUNT_ID, B.AGENT_PARENTID AS PARENT_ID, B.AGENT_SITEPURPOSE , B.CUSTOM_CUSTOMERNAME
-- ,B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY ,		
-- PRODUCT_AREA, COUNT(DISTINCT FEATURE_NAME) AS U_FEATURES, SUM(FEATURE_CLICKS) AS FEATURECLICKS, COUNT(DISTINCT VISITOR_ID)

-- FROM FEATURE_GROUPING F
-- RIGHT JOIN ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
-- where F.FEATURE_NAME is not null		
-- GROUP BY 1,2,3,4,5,6,7,8,9
-- ORDER BY FEATURECLICKS DESC
----------------------------------------------------------------------------------------------------------------------------------------------------
