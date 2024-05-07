

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)



# Baixa a chave secreta do código
assert_that(file.exists("keys/psql12-copy_prod_keySUPER.json"))
keys <- jsonlite::read_json("keys/psql12-homolog_keySUPER.json")


# Verifica se pode conectar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = keys$dbname,
                            host = keys$host,
                            port = keys$port,
                            user = keys$username, 
                            password = keys$password,
                            options="-c search_path=osc")

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(),
                    dbname = keys$dbname,
                    host = keys$host,
                    port = keys$port,
                    user = keys$username,
                    password = keys$password)

# connec <- dbConnect(RPostgres::Postgres(),
#                     dbname = "portal_osc2",
#                     host = "psql12",
#                     port = keys$port,
#                     user = "usrpublica",
#                     password = "k0IT18gLpMURDECJBmzY")


# Verifica a coneção com a base
assert_that(dbIsValid(connec))

# rm(keys, TestConexao)

keys

dbGetQuery(connec, "select * FROM PostGis_Full_Version()")
dbGetQuery(connec, "select * FROM public.PostGis_Full_Version()")





