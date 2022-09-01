library(DBI)
library(tidyverse)
library(googlesheets4)
library(lubridate)
con <- dbConnect(odbc::odbc(), "asternic", timeout = 10)


rcvdf <- dbGetQuery(con,"SELECT 
                          AGT.UNIQUEID ID,
                           DATE(DATETIME)DATA,
                            DATE_FORMAT(DATETIME, '%H') HORA,
                             CONVERT(WEEK(DATETIME, 5) - WEEK(DATETIME- INTERVAL DAY(DATETIME)-1 DAY, 5) + 1,DECIMAL) SEMANA,
                              CONVERT(LASTEDFORSECONDS,DECIMAL) DURACAO,
                               TEMPO_ESPERA,
                                QUEUE FILA,
                                 AGENT AGENTE,
                                  DATA TELEFONE,
                                   'ATENDIDA' TIPO
                                     FROM 
                                     
                                      agent_activity AGT,
                                       
                                        (SELECT UNIQUEID,
                                          CONVERT(INFO1,DECIMAL) TEMPO_ESPERA 
                                            FROM queue_stats_full
                                             WHERE
                                              DATE_FORMAT(DATETIME, '%Y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
                                               AND EVENT='CONNECT'
                                                AND QUEUE IN ('0800','799') ) AS WAIT
           
                                                  WHERE 
                                                   AGT.UNIQUEID=WAIT.UNIQUEID 
                                                    AND DATE_FORMAT(DATETIME,'%y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
                                                     AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
                                                      AND QUEUE IN ('0800','799')
                                                       ORDER BY datetime DESC") 



abdf <- dbGetQuery(con,"SELECT 
                          UNIQUEID ID,
                           DATE(DATETIME)DATA,
                            DATE_FORMAT(DATETIME, '%H') HORA,
                              CONVERT(WEEK(DATETIME, 5) - WEEK(DATETIME- INTERVAL DAY(DATETIME)-1 DAY, 5) + 1,DECIMAL) SEMANA,
                               CONVERT(INFO3,DECIMAL) DURACAO,
                                CONVERT(0,DECIMAL) TEMPO_ESPERA,
                                 QUEUE FILA,
                                  AGENT AGENTE,
                                   '' TELEFONE,
                                    'PERDIDA' TIPO
                                      FROM queue_stats_full 
                                       WHERE 
                                        DATE_FORMAT(DATETIME,'%y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
                                         AND EVENT='ABANDON'
                                          AND INFO3>=9
                                           AND QUEUE IN ('0800','799')
                                            ORDER BY datetime DESC ")


trsff <- dbGetQuery(con,"SELECT 
                          AGT.UNIQUEID ID,
                           DATE(DATETIME)DATA,
                            DATE_FORMAT(DATETIME, '%H') HORA,
                             CONVERT(WEEK(DATETIME, 5) - WEEK(DATETIME- INTERVAL DAY(DATETIME)-1 DAY, 5) + 1,DECIMAL) SEMANA,
                              CONVERT(LASTEDFORSECONDS,DECIMAL) DURACAO,
                               TEMPO_ESPERA,
                                QUEUE FILA,
                                 AGENT AGENTE,
                                  DATA TELEFONE,
                                   'TRANSFERIDA' TIPO
                                     FROM
                                  
                                      agent_activity AGT,
                                       
                                        (SELECT UNIQUEID,
                                          CONVERT(INFO1,DECIMAL) TEMPO_ESPERA 
                                            FROM queue_stats_full
                                             WHERE
                                              DATE_FORMAT(DATETIME, '%Y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
                                               AND EVENT='CONNECT'
                                                AND QUEUE IN ('0800','799') ) AS WAIT
                                               
                                              WHERE 
                                              
                                                AGT.UNIQUEID=WAIT.UNIQUEID 
                                                    AND DATE_FORMAT(DATETIME,'%y-%m-%d')>= MAKEDATE(YEAR(NOW()),1)
                                                     AND INFO1 ='TRANSFER'
                                                      AND QUEUE IN ('0800','799')
                                                       ORDER BY datetime DESC ")


agentes <- read_sheet("1Y7ciYhCMSVKs9F9fzhuHCHyn74gcMHegNiqlyxDdTv0",sheet="AGENTENOMES",range ="A:B" )


dtf <- rbind(rcvdf,abdf,trsff)


dtf2 <- left_join(dtf,agentes,by="AGENTE") 


dtf3 <- dtf2 %>% group_by(MES=floor_date(DATA,"month"),
                            ID,
                             SEMANA,
                              DATA,
                               HORA,
                                FILA,
                                 TIPO,
                                  AGENTE,
                                   TELEFONE,
                                    DURACAO,
                                     TEMPO_ESPERA,
                                      NOME) %>% 
                                       summarize(n=n_distinct(ID))

sheet_write(dtf2,ss="1Y7ciYhCMSVKs9F9fzhuHCHyn74gcMHegNiqlyxDdTv0",sheet="DADOS")




