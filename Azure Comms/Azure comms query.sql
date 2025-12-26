
-- CA accounts 
-- Files specific usage
-- Full usage vs Files Usage 
-- Revenue
 SELECT id
 --, CUSTOM_CUSTOMERNAME
--  , CUSTOM_CUSTOMERNAME,  AGENT_INSTANCECATEGORY, AGENT_INSTANCETYPE ,          		
--     CUSTOM_TR_MARKET_SEGMENT_L_1,CUSTOM_TR_MARKET_SEGMENT_L_2, CUSTOM_COUNTRY	
    FROM PROD.SOURCE.PENDO_HIGHQ_ACCOUNT_HISTORY_VW		
    WHERE ID IS NOT NULL and ID not like '%[a-z]%' 		
    --AND AGENT_INSTANCECATEGORY LIKE 'Client' AND AGENT_INSTANCETYPE LIKE 'Live'		

    and CUSTOM_SERVERNAME LIKE ANY (

        'https://apmea.elm.modaxo.com/',
'https://apvoh.ey.com/',
'https://aus.highqdataroom.com/',
'https://baggins.highq.com/',
'https://bunzl.highq.com/',
'https://cambria.highq.com/',
'https://circor.highq.com/',
'https://clientconnect.paulweiss.com/',
'https://collaborate.ashfords.co.uk/',
'https://collaborate.lawblacks.com/',
'https://collaborate.mccarter.com/',
'https://collaborate.nelsonmullins.com/',
'https://collaborate.parkerpoe.com/',
'https://collaborate.rwkgoodman.com/',
'https://cportal.kingsleynapley.co.uk/',
'https://dataroom.bto.co.uk/',
'https://dataroom.stephens-scown.co.uk/',
'https://dataroom.vbk.nl/',
'https://datarooms.triplaw.nl/',
'https://delos.highq.com/',
'https://dynappix.highq.com/',
'https://ftmyerscao.highq.com/',
'https://hub.teeslaw.com/',
'https://mindbody.highq.com/',
'https://nixonpeabody.highq.com/',
'https://nmac.highq.com/',
'https://online.dcslegal.com/',
'https://opflegal.op-f.org/',
'https://portal.tughans.com/',
'https://share.proskauer.com/',
'https://terraco.highq.com/',
'https://testshare.proskauer.com/',
'https://traempartners.highq.com/',
'https://traversportal.traverssmith.net/',
'https://unilibertonia.highq.com/',
'https://vedderprice.highq.com/',
'https://viseu.highq.com/',
'https://bishopcarter.highq.com/'

    )
QUALIFY RANK() OVER (PARTITION BY ID ORDER BY LAST_UPDATED_AT DESC)= 1

