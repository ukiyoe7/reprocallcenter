
library(DBI)
library(dplyr)
library(googlesheets4)
library(readr)

con2 <- dbConnect(odbc::odbc(), "reproreplica")

## PRODUTIVIDADE_ATENDIMENTO_APONTAMENTO

df_pdtv_apt <- dbGetQuery(con2, statement = read_file("C:\\Users\\Repro\\Documents\\R\\ATENDIMENTO\\reprocallcenter\\SQL\\PRODUTIVIDADE_ATENDIMENTO_APONTAMENTO.sql"))

View(df_pdtv_apt)
  

localped <- dbGetQuery(con2,"SELECT * FROM LOCALPED")

View(localped)




