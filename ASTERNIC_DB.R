library(DBI)
con2 <- dbConnect(odbc::odbc(), "asternic", timeout = 10)



queue_stats_full <- dbGetQuery(con2,"
           SELECT *
           FROM queue_stats_full
           WHERE
           DATE_FORMAT(DATETIME, '%Y-%m-%d')=CURDATE()")



View(queue_stats_full)


agent_activity <- dbGetQuery(con2,"
           SELECT *
           FROM agent_activity
           WHERE
           DATE_FORMAT(DATETIME, '%Y-%m-%d')='2022-09-27'")



View(agent_activity)