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
KeyFile <- "keys/rais_2019_MuriloJunqueira.json"
assert_that(file.exists(KeyFile))
keys <- jsonlite::read_json(KeyFile)


# Verifica se pode conectar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = keys$dbname,
                            host = keys$host,
                            port = keys$port,
                            user = keys$username, 
                            password = keys$password,
                            options="-c search_path=rfb_2024")

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
                    options="-c search_path=rfb_2024")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

# rm(keys, TestConexao, KeyFile)


# Testa extrair dados do banco
Teste <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_dados_gerais",
                               # " LIMIT 500", 
                               " WHERE id_osc = 402143",
                               ";")))


Tables <- dbListTables(connec)
Tables

# Testa extrair dados do banco
Teste <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM estabelecimento",
                               " LIMIT 500", 
                               ";")))
names(Teste)
View(Teste)

class(Teste$cnpj)


Teste2 <- try(dbGetQuery(connec, 
                         paste0("SELECT * FROM estabelecimento",
                                # " LIMIT 500", 
                                " WHERE cnpj = '08931561000116'",
                                ";")))
