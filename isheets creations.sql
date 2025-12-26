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
A.AGENT_PARENTID= B.AGENT_PARENTID),

FEATURE_EVENTS AS (		
    SELECT		
    TIMESTAMP,
    --cast(TIMESTAMP AS DATE) AS DDMMYYYY,		
    --CONCAT(day(TIMESTAMP),':',MONTH(TIMESTAMP),':', YEAR(TIMESTAMP)) AS DDMMYYYY,    		
    account_id, 		
    visitor_id, 		
    FEATURE_ID, 		
    SUM(num_events) as feature_clicks		
	FROM	prod.source.pendo_highq_feature_event_vw
	WHERE	APP_ID LIKE '-323232' AND to_date(convert_timezone('UTC','America/New_York',timestamp::timestamp_ntz)) BETWEEN '2025-01-01' AND '2025-07-31'    
    GROUP BY 1,2,3,4		
   )		

-- Query to get # of active users in time period
-- select count(distinct a.AGENT_PARENTID), count(distinct a.accountid) 
-- from all_accounts a
-- right join feature_events f on a.accountid=f.account_id

, feature_id as ( 		
 SELECT	id,name,group_id	
	FROM prod.source.pendo_highq_feature_history_vw	
    where name 		
    like any(		
    'iSheets - Add',		
    'iSheets - Add - iSheet',		
    'iSheets - Add - iSheet - Save',		
    'iSheets - Add - Template',		
    'iSheets - Add Template - Import Button',		
    'iSheets - Add - From an Excel file',		
    'isheets - Import from an Excel - Review/Import',
    'isheets - Import from an Excel - Review/Import - Save'
    )		
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
    )		

,		
product_area as(		
    select ID as group_id, Name from PROD.SOURCE.PENDO_HIGHQ_GROUP_VW		
    qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		
)		
,
visitor as (
SELECT ID, AGENT_VISITORROLE AS SYS_ROLE FROM PROD.SOURCE.PENDO_HIGHQ_VISITOR_HISTORY_VW 
qualify RANK() OVER (PARTITION BY id ORDER BY last_updated_at DESC)= 1		)


SELECT 
F.TIMESTAMP, F.ACCOUNT_ID, B.AGENT_PARENTID AS PARENT_ID, B.AGENT_SITEPURPOSE ,B.AGENT_ISSITETEMPLATE, B.CUSTOM_CUSTOMERNAME,  B.AGENT_INSTANCECATEGORY, B.AGENT_INSTANCETYPE ,          		
B.CUSTOM_TR_MARKET_SEGMENT_L_1,B.CUSTOM_TR_MARKET_SEGMENT_L_2, B.CUSTOM_COUNTRY ,		
F.VISITOR_ID,V.SYS_ROLE	,	
FID.NAME AS FEATURE_NAME, F.FEATURE_CLICKS, PA.NAME AS PRODUCT_AREA		
--F.* , FID.NAME,PA.NAME AS PRODUCT_AREA		
FROM FEATURE_EVENTS F		
RIGHT join ALL_ACCOUNTS b on F.ACCOUNT_ID = B.ACCOUNTID		
RIGHT JOIN FEATURE_ID FID ON F.FEATURE_ID =FID.ID		
LEFT JOIN product_area PA ON FID.GROUP_ID = PA.group_id		
left join visitor v on v.ID = F.VISITOR_ID		
where fid.name is not null	