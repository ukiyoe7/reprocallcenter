## CALL CENTER FILIAIS 

library(DBI)
library(tidyverse)
library(googlesheets4)

gs4_auth_configure(api_key = "AIzaSyCvfBeudlRFCviOsk7Glu6Snka10HkyiLUcon")


con2 <- dbConnect(odbc::odbc(), "asternic", timeout = 10)



## CRICIUMA ========================================================================================

#Received calls


rcvd_CRI <- dbGetQuery(con2,"
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
                           AND QUEUE IN ('3000') ) AS WAIT
                           
                            WHERE 
                             AGT.UNIQUEID=WAIT.UNIQUEID 
                              AND DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()
                               AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
                                AND QUEUE IN ('3000')
                                 ORDER BY datetime DESC")


#abandoned calls

abd_CRI <- dbGetQuery(con2,"SELECT 
                             DATE_FORMAT(datetime, '%H') HORA,
                              DATETIME DATETIME,
                               QUEUE QUEUE,
                                AGENT AGENT,
                                 CONVERT(INFO3,DECIMAL) LASTEDFORSECONDS,
                                  'PERDIDA' TIPO,
                                    INFO3 ESPERA
                                     FROM queue_stats_full 
                                      WHERE 
                                       DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                        AND EVENT='ABANDON'
                                         AND INFO3>=9 AND
                                           QUEUE IN ('3000')
                                            ORDER BY datetime DESC")

#transferred calls

trsf_CRI <- dbGetQuery(con2,"SELECT 
                              DATE_FORMAT(datetime, '%H') HORA,
                               DATETIME,
                                QUEUE,
                                 AGENT,
                                  CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
                                   'TRANSFERIDA' TIPO,
                                     CONVERT(0,DECIMAL) ESPERA 
                                      FROM agent_activity 
                                       WHERE 
                                        DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                         AND INFO1='TRANSFER'
                                          AND QUEUE IN ('3000')
                                           ORDER BY datetime DESC")

dt_CRI <-rbind(rcvd_CRI,abd_CRI,trsf_CRI) %>% mutate(FILIAL='CRICIUMA')

## CHAPECO ========================================================================================

#Received calls


rcvd_CHA <- dbGetQuery(con2,"
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
                           AND QUEUE IN ('4000') ) AS WAIT
                           
                            WHERE 
                             AGT.UNIQUEID=WAIT.UNIQUEID 
                              AND DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()
                               AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
                                AND QUEUE IN ('4000')
                                 ORDER BY datetime DESC")


#abandoned calls

abd_CHA <- dbGetQuery(con2,"SELECT 
                             DATE_FORMAT(datetime, '%H') HORA,
                              DATETIME DATETIME,
                               QUEUE QUEUE,
                                AGENT AGENT,
                                 CONVERT(INFO3,DECIMAL) LASTEDFORSECONDS,
                                  'PERDIDA' TIPO,
                                    INFO3 ESPERA
                                     FROM queue_stats_full 
                                      WHERE 
                                       DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                        AND EVENT='ABANDON'
                                         AND INFO3>=9 AND
                                           QUEUE IN ('4000')
                                            ORDER BY datetime DESC")

#transferred calls

trsf_CHA <- dbGetQuery(con2,"SELECT 
                              DATE_FORMAT(datetime, '%H') HORA,
                               DATETIME,
                                QUEUE,
                                 AGENT,
                                  CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
                                   'TRANSFERIDA' TIPO,
                                     CONVERT(0,DECIMAL) ESPERA 
                                      FROM agent_activity 
                                       WHERE 
                                        DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                         AND INFO1='TRANSFER'
                                          AND QUEUE IN ('4000')
                                           ORDER BY datetime DESC")

dt_CHA <-rbind(rcvd_CHA,abd_CHA,trsf_CHA) %>% mutate(FILIAL='CHAPECO')


dt <- union_all(dt_CRI,dt_CHA)

## JOINVILLE ========================================================================================

#Received calls


rcvd_JOI <- dbGetQuery(con2,"
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
                           AND QUEUE IN ('2000') ) AS WAIT
                           
                            WHERE 
                             AGT.UNIQUEID=WAIT.UNIQUEID 
                              AND DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()
                               AND INFO1 IN ('COMPLETECALLER','COMPLETEAGENT')
                                AND QUEUE IN ('2000')
                                 ORDER BY datetime DESC")


#abandoned calls

abd_JOI <- dbGetQuery(con2,"SELECT 
                             DATE_FORMAT(datetime, '%H') HORA,
                              DATETIME DATETIME,
                               QUEUE QUEUE,
                                AGENT AGENT,
                                 CONVERT(INFO3,DECIMAL) LASTEDFORSECONDS,
                                  'PERDIDA' TIPO,
                                    INFO3 ESPERA
                                     FROM queue_stats_full 
                                      WHERE 
                                       DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                        AND EVENT='ABANDON'
                                         AND INFO3>=9 AND
                                           QUEUE IN ('2000')
                                            ORDER BY datetime DESC")

#transferred calls

trsf_JOI <- dbGetQuery(con2,"SELECT 
                              DATE_FORMAT(datetime, '%H') HORA,
                               DATETIME,
                                QUEUE,
                                 AGENT,
                                  CONVERT(LASTEDFORSECONDS,DECIMAL) LASTEDFORSECONDS,
                                   'TRANSFERIDA' TIPO,
                                     CONVERT(0,DECIMAL) ESPERA 
                                      FROM agent_activity 
                                       WHERE 
                                        DATE_FORMAT(DATETIME, '%Y-%m-%d') =CURDATE()
                                         AND INFO1='TRANSFER'
                                          AND QUEUE IN ('2000')
                                           ORDER BY datetime DESC")

dt_JOI <-rbind(rcvd_JOI,abd_JOI,trsf_JOI) %>% mutate(FILIAL='JOINVILLE')

## ==================================================================================================


## UNION ALL DATA

dt <- union_all(dt_CRI,dt_CHA) %>%  union_all(.,dt_JOI)

## WRITE ON GGSHEETS

sheet_write(dt,ss="1tOBm4Vdoh7_P3UUOHHryeLH0HChsd7FMUhFnMkovVPQ",sheet="DADOS")
