
library(DBI)
con2 <- dbConnect(odbc::odbc(), "asternic", timeout = 10)
library(googlesheets4)



fila_ativos <- dbGetQuery(con2,"
           SELECT *
           FROM agent_activity
           WHERE
           info1<>'' AND 
           DATE_FORMAT(DATETIME, '%Y-%m-%d')>='2022-09-01'
           AND QUEUE LIKE '%ATIVO%'
                          ")



View(fila_ativos)


fila_ativos_failed <- dbGetQuery(con2,"
           SELECT *
           FROM agent_activity
           WHERE
           info1<>'' AND 
           DATE_FORMAT(DATETIME, '%Y-%m-%d')>='2022-09-01'
           AND QUEUE LIKE '%ATIVO%' AND EVENT='FAILED OUT'
                          ")



View(fila_ativos_failed)

sheet_write(fila_ativos,ss="1CK7KeITe7eu1xooA0DLxHEx3Py2cMk4MCb6svQW_7OY",sheet="ATIVAS")

sheet_write(fila_ativos_failed ,ss="1CK7KeITe7eu1xooA0DLxHEx3Py2cMk4MCb6svQW_7OY",sheet="FALHAS")

