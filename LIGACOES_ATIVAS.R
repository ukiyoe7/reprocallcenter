library(DBI)
library(tidyverse)
library(googlesheets4)
con <- dbConnect(odbc::odbc(), "asternic", timeout = 10)

## LIGAÇÕES ATIVAS =====================

fila_ativos <- dbGetQuery(con,"
 SELECT 
            DATE_FORMAT(AGT.datetime, '%d') DIA,
            DATETIME,
            QUEUE,
            AGENT,
            CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
           'ATIVA' TIPO

           FROM agent_activity AGT
           WHERE 
            MONTH(DATETIME) = MONTH(CURRENT_DATE())
           AND YEAR(DATETIME) = YEAR(CURRENT_DATE())
           AND QUEUE LIKE '%ATIVO%'
           AND INFO1 LIKE '%COMPLET%'
           ORDER BY datetime DESC")

##  ATIVAS =====================


fila_ativos_failed <- dbGetQuery(con,"
           SELECT
            DATE_FORMAT(AGT.datetime, '%d') DIA,
            DATETIME,
            QUEUE,
            AGENT,
            CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
           'PERDIDAS' TIPO
           FROM agent_activity AGT
           WHERE
           info1<>'' 
           AND MONTH(DATETIME) = MONTH(CURRENT_DATE())
           AND YEAR(DATETIME) = YEAR(CURRENT_DATE())
           AND QUEUE LIKE '%ATIVO%' AND EVENT='FAILED OUT'
                          ")

ativos <- rbind(fila_ativos,fila_ativos_failed)

sheet_write(ativos,ss="1KYc7CMSBLIAsnMMTBRzTjfaraXJLr9HjnnEE3KPvT20",sheet="DADOS")



