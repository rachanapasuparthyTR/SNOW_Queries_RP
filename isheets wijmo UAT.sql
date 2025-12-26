WITH		
parents as (		
   SELECT ID AS AGENT_PARENTID, CUSTOM_CUSTOMERNAME,  AGENT_INSTANCECATEGORY, AGENT_INSTANCETYPE ,          		
    CUSTOM_TR_MARKET_SEGMENT_L_1,CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY, CUSTOM_SERVERNAME
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE 
    --ID IS NOT NULL and ID not like '%[a-z]%' 		
    --AND CUSTOM_TR_MARKET_SEGMENT_L_1 LIKE  'Corporate'		
    --AND AGENT_INSTANCECATEGORY LIKE 'Client' AND AGENT_INSTANCETYPE LIKE 'Live'		
    CUSTOM_SERVERNAME LIKE ANY (
        'https://esterauat.highq.com/',
        'https://aosuatcollaborate.highq.com/',
        'https://uatcollaborate.allens.com.',
        'https://bclpuat.highq.com/',
        'https://clonetwobirds.highq.com/',
        'https://brownejacobsonclone.highq.com/',
        'https://restoreclarkewillmott.highq.com/',
        'https://collaborate15sg1.highq.com/',
        'https://ccasuat.highq.com/',
        'https://collab4officemobile.highq.com/',
        'https://collaboratev4.highq.com/',
        'https://collaborate5aus2.highq.com/',
        'https://eversheds-sutherlanduat.highq.com/',
        'https://dellauat.highq.com/',
        'https://uatdentons.highq.com/',
        'https://dlavroomsuat.highqsolutions.com/',
        'https://eigendemo.highq.com/',
        'https://clonefootanstey.highq.com/',
        'https://freshfieldsuat.highq.com/',
        'https://uatlegalknowledgehsbc.highq.com/',
        'https://betademo.highq.com/',
        'https://devbuilds.highq.com/',
        'https://v4ausuat.highq.com/',
        'https://highqdftest01.highq.com/',
        'https://intranet.highq.com/',
        'https://collaboratebeta.highq.com/',
        'https://psdemo.highq.com/',
        'https://workflowuat.highq.com/',
        'https://collaborate3uat.highqsolutions.com/',
        'https://partners.highq.com/',
        'https://integrationbeta.highq.com/',
        'https://securitytest.highqsolutions.com/',
        'https://training.highq.com/',
        'https://collaborateuat.highqsolutions.com/',
        'https://collaboratev3.highqsolutions.com/',
        'https://homedev.highq.com/',
        'https://learn.highq.com/',
        'https://supportbeta.highq.com/',
        'https://hsfcollaborate.highqsolutions.com/',
        'https://integrationlive.highq.com/',
        'https://integrationprevious.highq.com/',
        'https://trainingbeta.highq.com/',
        'https://kirademo.highq.com/',
        'https://levertondemo.highq.com/',
        'https://clone1linklaters.highq.com/',
        'https://protolpe1.highq.com/',
        'https://protolpe2.highq.com/',
        'https://protolpe3.highq.com/',
        'https://macquarieuat.highqsolutions.com/',
        'https://sandboxproductmatterspherecol1.highq.com/',
        'https://mercedes-benz-uat.highq.com/',
        'https://monumentuat.highq.com/',
        'https://nrdealroomsclone.highq.com/',
        'https://cloneoc.highq.com/',
        'https://workflowdev.highq.com/',
        'https://testperkins.highq.com/',
        'https://sdhquat.highq.com/',
        'https://ce-eu-sandbox.highq.com/',
        'https://aosuat.highq.com/',
        'https://supportteam.highq.com/',
        'https://supportuatv4.highq.com/',
        'https://globalfirm.highq.com/',
        'https://cls-ps.highq.com/',
        'https://homedemo.highq.com/',
        'https://thoughtriveruat.highq.com/',
        'https://tltsolicitorsclone3.highq.com/',
        'https://acme.highq.com/',
        'https://acmecorp.highq.com/',
        'https://ukwbduat.highq.com/'

    )
    QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
    )
-- ,ACCOUNTS_1 AS (		
--     SELECT ID AS ACCOUNTID, AGENT_PARENTID, AGENT_SITEPURPOSE ,AGENT_ISSITETEMPLATE,LAST_UPDATED_AT		
		
--     FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
--     WHERE 
--     --ID IS NOT NULL and ID not like '%[a-z]%' 		
--     --AND CONTAINS(lower(AGENT_SITEPURPOSE),'client portal')		
--     --AND AGENT_SITEPURPOSE LIKE 'Contract Management'		
--     --AND 
--     AGENT_PARENTID IN (SELECT AGENT_PARENTID FROM parents) 		
--     QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1 		
-- )

SELECT AGENT_PARENTID FROM parents;
-- select * from PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW where id like '1445964615120'
-- QUALIFY rank() over (partition by id order by last_updated_at desc)=1;

