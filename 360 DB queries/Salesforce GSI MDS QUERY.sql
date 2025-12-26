

CREATE or replace TABLE RP_SALESFORCE_GSI (

"Account Name" STRING,

"Case Number" INT,

"Case Record Type" STRING,

created_date TIMESTAMP_TZ,

CLOSED_DATE TIMESTAMP_TZ,

Country STRING,

--ESC_INQUIRY_REASON_C,

esc_product_title_c STRING,

PRIORITY STRING, 

ESC_IMPACT_C STRING,

ESC_STAGE_C STRING,

SUBJECT STRING,

DESCRIPTION STRING,

ESC_CASE_TYPE_C STRING

);
 
INSERT INTO RP_SALESFORCE_GSI ("Account Name","Case Number","Case Record Type",created_date,CLOSED_DATE,Country,esc_product_title_c,PRIORITY,ESC_IMPACT_C,ESC_STAGE_C,SUBJECT,DESCRIPTION,ESC_CASE_TYPE_C

) 

SELECT 

NAME as "Account Name",

case_number AS "Case Number",

ESC_RECORD_TYPE_C as "Case Record Type",

cs.created_date,

cs.CLOSED_DATE,

LCRM_COUNTRY_C AS "Country",

--ESC_INQUIRY_REASON_C,

esc_product_title_c,

PRIORITY, 

ESC_IMPACT_C,

--MANAGER_C,

ESC_STAGE_C,

SUBJECT,

CS.DESCRIPTION,

ESC_CASE_TYPE_C
 
FROM prod.source.salesforce_gsi_case_vw cs

inner join prod.source.salesforce_gsi_account_vw acc on acc.id = cs.account_id

WHERE 

    esc_product_title_c IN ('HighQ','Collaborate') 

    AND LOWER(name) NOT IN ('test', 'test, please ignore')

    AND LOWER(subject) != 'test'

    AND LOWER(cs.description) != 'spam'

    --case_number like '20158697'

    and lower(STATUS) NOT IN ('cancelled', 'duplicate','rejected','merged')

    and cs.created_date > '2024-01-01 00:00:00'

    AND lower(NAME) not like '%test %'

    AND ecm_party_id_c IS NOT NULL

    AND year(cs.CREATED_DATE)>=2024

    ;
 
 
    SELECT * FROM RP_SALESFORCE_GSI;
 