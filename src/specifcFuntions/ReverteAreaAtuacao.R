# Script para Reverter a última atualização da base tb_area_atuacao

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-08


library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)
library(assertthat)

# Conexão ####

# Baixa a chave secreta do código
KeyFile <- "keys/psql12-homolog_keySUPER.json"
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


# Reverte Atualização ####


# Limpar os dados colocados na atualização antiga (fazer um script só para isso)

### Evitar limpar dados dos usuários

## Baixar a base atual de tb_areas_atualizacao
# OldData <- readRDS("D:/Users/Murilo/Dropbox/ActiveProjects/IPEAGeral/data/temp/2024-04-09 Extracao MOSC/tb_area_atuacao.RDS")

# Testa extrair dados do banco
OldData <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_area_atuacao",
                               # " LIMIT 500", 
                               ";")))

names(OldData)

table(OldData$ft_area_atuacao)

## Ver pela fonte de atualização os dados que foram inseridos na última att e limpar.
## (usar a chave: "id_osc & cd_area_atuacao & cd_subarea_atuacao")

CleanData <- OldData %>% 
  mutate(IsUser = ifelse(ft_area_atuacao == "Representante de OSC", 1, 0)) %>% 
  group_by(id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
  arrange(desc(IsUser)) %>% 
  slice(1)

table(CleanData$ft_area_atuacao)


DeleteRow <- OldData %>% 
  mutate(IsUser = ifelse(ft_area_atuacao == "Representante de OSC", 1, 0)) %>% 
  group_by(id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
  arrange(IsUser) %>% 
  mutate(n = row_number(), 
         deleterow = ifelse(n > 1, 1, 0)) %>% 
  ungroup() %>% 
  select(id_area_atuacao, deleterow) %>% 
  select(everything())

table(DeleteRow$n)
table(DeleteRow$deleterow)

## Faz upload da tabela temporária com as linhas de atualização

if(dbExistsTable(connec, "update_temp")) dbRemoveTable(connec, "update_temp")

dbWriteTable(connec, "update_temp", DeleteRow)

# teste <- dbGetQuery(connec, "SELECT * FROM update_temp")
# View(teste)
# table(teste$temp_var, useNA = "always")

# Faz umm join desta tabela com os dados antigos

# Deleta a coluna, se ela existir:
query_DropCol <- paste0("ALTER TABLE tb_area_atuacao",
                        " DROP COLUMN IF EXISTS deleterow;")

dbExecute(connec, query_DropCol)

# Pega o tipo de variável a ser criado:
QueryGetInformation <- paste0("SELECT *", "\n",
                              "FROM information_schema.columns", "\n",
                              "WHERE table_name = 'tb_area_atuacao';")
# cat(QueryGetInformation)
ColTypes <- dbGetQuery(connec, QueryGetInformation)
VarType <- ColTypes$data_type[ColTypes$column_name == "deleterow"]

dbExecute(connec, query_AddCol)

rm(QueryGetInformation, ColTypes, VarType, query_AddCol)

# Insere a coluna com as linhas para deletar
query_JoinUpdate <- paste0("DELETE FROM tb_area_atuacao", "\n",
                           "  WHERE deleterow = 1",
                           ";")

# cat(query_JoinUpdate)

# Executa a inserção das colunas
dbExecute(connec, query_JoinUpdate)

# Determina a atualização da coluna com base no valor de temp_var
query_UpdateCol <- paste0("UPDATE ", Table_NameAntigo, "\n",
                          " SET ", col, " = temp_var",
                          ";")
# cat(query_UpdateCol)

# query_DeleteRows
LinhasAtualizadas <- dbExecute(connec, query_UpdateCol)

message("Linhas atualizadas na coluna ", col)

# Deleta a coluna criada
dbExecute(connec, query_DropCol)


# Fim ####
