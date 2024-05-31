# Estudo da base RAIS

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-05-27

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
                            options="-c search_path=vinculos_v6")

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
                    options="-c search_path=vinculos_v6")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao)

# Lista de Tabelas
Tables <- dbListTables(connec)

# Verifica dados dos vínculos ####

filepath <- "data/temp/2024-05-22 RAIS/2024-05-22 RAIS - vinculos/tb_vinculos_2021.RDS"
file.exists(filepath)
tb_vinculos_2021 <- readRDS(filepath)
names(tb_vinculos_2021)

# Verifica OSCs com voluntários ####

ExemploOSC <- fread("data/temp/2024-05-28 Estudo RAIS/OSCExemplosRAIS.csv")


tb_dados_gerais <- readRDS("backup_files/2023_01/output_files/tb_dados_gerais.RDS")

names(tb_dados_gerais)

CNPJOSC <- tb_dados_gerais %>% 
  dplyr::filter(id_osc %in% ExemploOSC$id_osc[ExemploOSC$AnoBase == 2019]) %>% 
  select(cd_identificador_osc) %>% 
  unlist() %>% as.character()

"04403747000141" %>% nchar()

"60121621000155" %>% nchar()



# Baixa dados dos exemplos:
rawData <- dbGetQuery(connec, 
                      paste0("SELECT * FROM tb_vinculos_2019 ",
                             "WHERE id_estab IN (", 
                             paste0("'", paste0(CNPJOSC, collapse = "', '"), "'"),
                             ")",
                             ";"))

saveRDS(rawData, "backup_files/2023_01/input_files/RAIS/Exemplo2019.RDS")



# Verifica OSC com voluntários na base dos Estabilecimentos ####

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
                            options="-c search_path=estabelecimentos")

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
                    options="-c search_path=estabelecimentos")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao)

# Lista de Tabelas
Tables <- dbListTables(connec)

Tables

# Baixa dados dos exemplos:
checaBase <- dbGetQuery(connec, 
                      paste0("SELECT * FROM tb_estabelecimentos_2017 ",
                             "LIMIT 1000",
                             ";"))


class(checaBase$cnpj_cei)

names(checaBase)

nchar("26163834000101")

checaBase$cnpj_cei[1:15]




tb_dados_gerais$cd_identificador_osc[tb_dados_gerais$id_osc == 1325701] %>% 
  as.character()

nchar("12264645000110")

# Baixa dados dos exemplos:
rawData <- dbGetQuery(connec, 
                      paste0("SELECT * FROM tb_estabelecimentos_2017 ",
                             "WHERE natureza_juridica IN ('3069', '3220', '3301', '3999')",
                             ";"))
names(rawData)

Teste <- rawData %>% 
  # dplyr::filter(cnpj_cei == 37378492000163)
  mutate()

rawData["12264645000110"]






# Fim ####