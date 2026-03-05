


library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)

# Chaves do banco de dados
keys <- "keys/psql12-homolog_key.json" # Banco de homologação
# keys <- "keys/psql12-usr_manutencao_mapa.json" # Banco de produção

# Concecta aos bancos de dados do MOSC:
source("src/generalFunctions/postCon.R") 
conexao_mosc <- postCon(keys, 
                        Con_options = "-c search_path=osc")

dbIsValid(conexao_mosc)

rm(postCon, keys)


# Informações sobre o banco de dados  
tables <- dbListTables(conexao_mosc)

# tables

# rm(tables)

# Verifica processos que estão travando o banco
teste <- try(
  dbGetQuery(
    conexao_mosc,
    "
SELECT pid, now() - query_start AS duration, query, state
FROM pg_stat_activity
WHERE state != 'idle' AND query NOT LIKE '%pg_stat_activity%';
"
  )
)

teste



# Interrompe processos:
dbExecute(conexao_mosc, "SELECT pg_terminate_backend(122835);")

dbExecute(conexao_mosc, "SELECT pg_terminate_backend(38417);")



source("development_zone/2026-02-12 Teste/ReiniciaRotina.R")



teste2 <- try(
  dbGetQuery(
    conexao_mosc,
    "
SELECT blocked_locks.pid AS blocked_pid,
       blocking_locks.pid AS blocking_pid,
       blocked_activity.query AS blocked_query,
       blocking_activity.query AS blocking_query
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database = blocked_locks.database
    AND blocking_locks.relation = blocked_locks.relation
    AND blocking_locks.page = blocked_locks.page
    AND blocking_locks.tuple = blocked_locks.tuple
    AND blocking_locks.virtualxid = blocked_locks.virtualxid
    AND blocking_locks.transactionid = blocked_locks.transactionid
    AND blocking_locks.classid = blocked_locks.classid
    AND blocking_locks.objid = blocked_locks.objid
    AND blocking_locks.objsubid = blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
"
  )
)

teste2


# De forma profilática, desconecta do banco de dados
dbDisconnect(conexao_mosc)
rm(conexao_mosc)

rm(teste, tables)
gc()