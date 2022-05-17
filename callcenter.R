library(DBI)
library(tidyverse)
library(googlesheets4)
gs4_auth_configure(api_key = "AIzaSyCvfBeudlRFCviOsk7Glu6Snka10HkyiLUcon")
con <- dbConnect(odbc::odbc(), "ODBC Asternic", timeout = 10)


#Received calls


rcvd <- dbGetQuery(con,"
           SELECT 
            DATE_FORMAT(AGT.datetime, '%H') HORA,
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
           DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()
           AND EVENT='CONNECT'
           AND QUEUE IN ('0800','799') ) AS WAIT
           WHERE 
           AGT.UNIQUEID=WAIT.UNIQUEID 
           AND DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()
           AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
           AND
           QUEUE IN ('0800','799')
           ORDER BY datetime DESC")


#abandoned calls

abd <- dbGetQuery(con,"SELECT 
DATE_FORMAT(datetime, '%H') HORA,
           DATETIME DATETIME,
           QUEUE QUEUE,
           AGENT AGENT,
           CONVERT(INFO3,DECIMAL) LASTEDFORSECONDS,
           'PERDIDA' TIPO,
           0 ESPERA
           FROM queue_stats_full 
           WHERE 
           DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
           AND EVENT='ABANDON'
            AND
           QUEUE IN ('0800','799')
           ORDER BY datetime DESC ")

#transferred calls

trsf <- dbGetQuery(con,"SELECT 
     DATE_FORMAT(datetime, '%H') HORA,
           DATETIME,
           QUEUE,
           AGENT,
           CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
           'TRANSFERIDA' TIPO,
           0 ESPERA
           FROM agent_activity 
           WHERE 
           DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
           AND
           INFO1='TRANSFER'
           AND
           QUEUE IN ('0800','799')
           ORDER BY datetime DESC ")

dt <-rbind(rcvd,abd,trsf)

sheet_write(dt,ss="1XTwBJC8DkOg--gVLxQASdtfUz7F6V3LtFANIbxmim2Y",sheet="DADOS")
