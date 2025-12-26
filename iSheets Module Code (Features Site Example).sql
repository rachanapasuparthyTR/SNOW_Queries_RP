-- being used for isheets module code 
WITH		
parents as (		
   SELECT ID AS AGENT_PARENTID, CUSTOM_CUSTOMERNAME,  AGENT_INSTANCECATEGORY, AGENT_INSTANCETYPE ,          		
    CUSTOM_TR_MARKET_SEGMENT_L_1,CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY		
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    --AND CUSTOM_TR_MARKET_SEGMENT_L_1 LIKE  'Corporate'		
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
	WHERE	APP_ID LIKE '-323232' AND to_date(convert_timezone('UTC','America/New_York',timestamp::timestamp_ntz)) BETWEEN '2024-11-01' AND '2025-04-30'    
    GROUP BY 1,2,3,4		
   )		
		
, feature_id as ( 		
 SELECT	id,name,group_id	
	FROM prod.source.pendo_highq_feature_history_vw	
    where name 		
    like any(		
        'isheet-wijmo-select-all-checkbox',
        'isheet-wijmo-paggination-usage',
        'isheet-wijmo-column-filter',
        'isheet-wijmo-inline-edit',
        'isheet-wijmo-column-pin-button',
        'isheet-Wijmo-Column-Sort Updated'
    )		
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
    )		
		
    ,		
product_area as(		
    select ID as group_id, Name from PROD.SOURCE.PENDO_HIGHQ_GROUP_VW		
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
)		
		
SELECT F.DDMMYYYY, F.ACCOUNT_ID, B.AGENT_PARENTID AS PARENT_ID, B.AGENT_SITEPURPOSE ,B.AGENT_ISSITETEMPLATE, B.CUSTOM_CUSTOMERNAME,  B.AGENT_INSTANCECATEGORY, B.AGENT_INSTANCETYPE ,          		
B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY ,		
F.VISITOR_ID,		
FID.NAME AS FEATURE_NAME, F.FEATURE_CLICKS, PA.NAME AS PRODUCT_AREA		
--F.* , FID.NAME,PA.NAME AS PRODUCT_AREA		
FROM FEATURE_EVENTS F		
LEFT JOIN FEATURE_ID FID ON F.FEATURE_ID =FID.ID		
LEFT JOIN product_area PA ON FID.GROUP_ID = PA.group_id		
right join ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
where fid.name is not null		
order by feature_clicks desc
