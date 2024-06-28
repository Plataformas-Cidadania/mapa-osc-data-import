# Scrip para controlar as funções de atualização MOSC

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-03-01

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa a chave secreta do código
KeyFile <- "keys/psql12-prod_key.json"
assert_that(file.exists(KeyFile))
keys <- jsonlite::read_json(KeyFile)


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
                    password = keys$password,
                    options="-c search_path=osc")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao, KeyFile)


# Testa extrair dados do banco
Teste <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_osc",
                               #" LIMIT 500", 
                               ";")))


names(Teste)

nrow(Teste) - sum(Teste$bo_osc_ativa)

saveRDS(Teste, "data/temp/tb_osc_atual.RDS")


tb_dados_gerais <- try(dbGetQuery(connec, 
                                  paste0("SELECT * FROM tb_dados_gerais",
                                         #" LIMIT 500", 
                                         ";")))


saveRDS(tb_dados_gerais, "data/temp/tb_dados_gerais_atual.RDS")
