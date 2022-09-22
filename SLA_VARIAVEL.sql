SELECT 
            DATE_FORMAT(AGT.datetime, '%H') HORA,
            DATE_FORMAT(DATETIME, '%d/%m/%Y') DIA,
            DATETIME,
            QUEUE,
            AGENT,
            CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
           'ATENDIDA' TIPO,
            ESPERA
           FROM agent_activity AGT,
           (SELECT
            UNIQUEID,
             CONVERT(INFO1,DECIMAL) ESPERA 
           FROM queue_stats_full
           WHERE
           DATE_FORMAT(DATETIME, '%Y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
           AND EVENT='CONNECT'
           AND QUEUE IN ('0800','799') ) AS WAIT
           WHERE 
           AGT.UNIQUEID=WAIT.UNIQUEID 
           AND DATE_FORMAT(DATETIME, '%Y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
           AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
           AND
           QUEUE IN ('0800','799') UNION
                  SELECT 
DATE_FORMAT(datetime, '%H') HORA,
           DATE_FORMAT(DATETIME, '%d/%m/%Y') DIA,
           DATETIME DATETIME,
           QUEUE QUEUE,
           AGENT AGENT,
           CONVERT(INFO3,DECIMAL) LASTEDFORSECONDS,
           'PERDIDA' TIPO,
           INFO3 ESPERA
           FROM queue_stats_full 
           WHERE 
           DATE_FORMAT(DATETIME, '%Y-%m-%d') >= MAKEDATE(YEAR(NOW()),1)
           AND EVENT='ABANDON'
            AND INFO3>=9 AND
           QUEUE IN ('0800','799') UNION
            SELECT 
     DATE_FORMAT(datetime, '%H') HORA,
           DATE_FORMAT(DATETIME, '%d/%m/%Y') DIA,
           DATETIME,
           QUEUE,
           AGENT,
           CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
           'TRANSFERIDA' TIPO,
           
             CONVERT(0,DECIMAL) ESPERA 
           FROM agent_activity 
           WHERE 
           DATE_FORMAT(DATETIME, '%Y-%m-%d') >= MAKEDATE(YEAR(NOW()),1)
           AND
           INFO1='TRANSFER'
           AND
           QUEUE IN ('0800','799')