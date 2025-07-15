# Banco de dados para extraruma cópia do MOSC em tabelas separadas

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-04-09

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

keys_file <- "keys/psql12-usr_manutencao_mapa.json" # acesso completo ao banco de produção
# keys_file <- "keys/psql12-homolog_key.json"  # Banco de homologação
assert_that(file.exists(keys_file))
keys <- jsonlite::read_json(keys_file)


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

rm(keys, TestConexao, keys_file)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Extrai tabelas selecionadas ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Tabelas disponíveis
Tables <- dbListTables(connec)
Tables


# Tabelas a se extrair:

TablesExtract <- c("tb_dados_gerais", "tb_osc", "tb_area_atuacao", 
                   "tb_localizacao", "tb_contato", "tb_relacoes_trabalho")

DirExtract <- "development_zone/2025-03-06 extract-homolog/"

if(!dir.exists(DirExtract)) {
  dir.create(DirExtract)
}

for (i in seq_along(TablesExtract)) {
  # i <- 1
  print(TablesExtract[i])
  
  DataLoad <- dbGetQuery(connec, paste0("SELECT * FROM ", 
                                        TablesExtract[i],
                                        # " LIMIT 500", 
                                        ";"))
  
  saveRDS(DataLoad, paste0(DirExtract, TablesExtract[i], ".RDS"))
  rm(DataLoad)
}
rm(i)


dbDisconnect(connec)
rm(TablesExtract, DirExtract, Tables, TablesExtract)
rm(connec)

# Fim ####
