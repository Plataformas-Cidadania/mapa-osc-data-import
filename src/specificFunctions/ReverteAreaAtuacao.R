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
library(DBI)
library(RODBC)
library(RPostgres)

# Variável para indicar que a atualização é teste

isTest <- FALSE

# Conexão ####

source("src/generalFunctions/postCon.R")

connec <- postCon("keys/psql12-prod_key.json", 
                  Con_options = "-c search_path=osc")

rm(postCon)

# Reverte Atualização ####

# Limpar os dados colocados na atualização antiga

## Baixar a base atual de tb_areas_atualizacao
# OldData <- readRDS("D:/Users/Murilo/Dropbox/ActiveProjects/IPEAGeral/data/temp/2024-04-09 Extracao MOSC/tb_area_atuacao.RDS")

# Testa extrair dados do banco
OldData <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_area_atuacao",
                               # " LIMIT 500", 
                               ";")))
# Checa a base:
## names(OldData)
## table(OldData$ft_area_atuacao)

# Ao invés te atualizar a base mesmo, testar nessa cópia:
if(isTest) {
  if(dbExistsTable(connec, "tb_area_atuacao_teste")) dbRemoveTable(connec, "tb_area_atuacao_teste")
  dbWriteTable(connec, "tb_area_atuacao_teste", OldData)
  
  NomeTabelaAtt <- "tb_area_atuacao_teste"
} else {
  if(dbExistsTable(connec, "tb_area_atuacao_backup")) dbRemoveTable(connec, "tb_area_atuacao_backup")
  dbWriteTable(connec, "tb_area_atuacao_backup", OldData)
  
  NomeTabelaAtt <- "tb_area_atuacao"
}

## Ver pela fonte de atualização os dados que foram inseridos na última att e limpar.

DeleteRow <- OldData %>% 
  # Usar a chave: ("id_osc & cd_area_atuacao & cd_subarea_atuacao")
  group_by(id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
  # Evitar limpar dados dos usuários
  mutate(IsUser = ft_area_atuacao == "Representante de OSC") %>% 
  arrange(IsUser) %>% 
  # Determinar que só uma linha da combinação de chaves pode ficar
  mutate(n = row_number(), 
         deleterow = n > 1) %>% 
  ungroup() %>% 
  select(id_area_atuacao, deleterow) %>% 
  select(everything())

# Checa os dados:
table(DeleteRow$deleterow)

# Deleta os dados usando uma entidade de dados temporária
if(dbExistsTable(connec, "update_temp")) dbRemoveTable(connec, "update_temp")
dbWriteTable(connec, "update_temp", DeleteRow)
dbExistsTable(connec, "update_temp") # checa criação de tabela

# Testa para ver se a base temporára está correta:
teste <- dbGetQuery(connec, "SELECT * FROM update_temp")
table(teste$deleterow, useNA = "always")
rm(teste)

# Deleta a coluna que indica os dados que vão ser deletados (se precisar):
query_DropCol <- paste0("ALTER TABLE ", NomeTabelaAtt,
                        " DROP COLUMN IF EXISTS deleterow;")
# cat(query_DropCol)
dbExecute(connec, query_DropCol)

# Cria coluna na tabela de dados antigos
query_AddCol <- paste0("ALTER TABLE ", NomeTabelaAtt,
                       " ADD COLUMN deleterow boolean",
                       ";")
# cat(query_AddCol)
dbExecute(connec, query_AddCol)
rm(query_AddCol)

# Testa para ver se a coluna foi inserida:
teste2 <- try(dbGetQuery(connec, 
                          paste0("SELECT * FROM ", NomeTabelaAtt,
                                 " LIMIT 500", 
                                 ";")))
assert_that("deleterow" %in% names(teste2))
rm(teste2)

# Insere a referência das linhas que vão ser inseridas:
query_JoinUpdate <- paste0("UPDATE ", NomeTabelaAtt, "\n",
                           " SET deleterow = update_temp.deleterow", "\n",
                           " FROM update_temp", "\n",
                           " WHERE ", NomeTabelaAtt, ".id_area_atuacao",  
                           " = update_temp.id_area_atuacao", 
                           ";")
# cat(query_JoinUpdate)
dbExecute(connec, query_JoinUpdate)

teste3 <- try(dbGetQuery(connec, 
                         paste0("SELECT * FROM ", NomeTabelaAtt,
                                # " LIMIT 500", 
                                ";")))

names(teste3)
# head(teste3)
table(teste3$deleterow, useNA = "always")
rm(teste3)

rm(query_JoinUpdate)

# Insere a coluna com as linhas para deletar
query_Delete <- paste0("DELETE FROM ", NomeTabelaAtt, "\n",
                           "  WHERE deleterow",
                           ";")
# cat(query_Delete)

# Deleta as linhas
dbExecute(connec, query_Delete)

# Verifica se deu tudo certo
teste4 <- try(dbGetQuery(connec, 
                         paste0("SELECT * FROM ", NomeTabelaAtt,
                                # " LIMIT 500", 
                                ";")))

names(teste4)
nrow(teste4)
table(teste4$deleterow, useNA = "always")
rm(teste4)

rm(query_Delete)

# Retira a coluna temporária para indicar as linhas a se deletar
dbExecute(connec, query_DropCol)
rm(query_DropCol)

# Vou deixar aqui opção de fazer uma cópia do output do teste
if(isTest) {
  # Verifica se deu tudo certo
  NewTable <- try(dbGetQuery(connec, 
                             paste0("SELECT * FROM ", NomeTabelaAtt,
                                    # " LIMIT 500", 
                                    ";")))
  
  DirExtract <- "data/temp/2024-07-08 extract/"
  if(!dir.exists(DirExtract)) {
    dir.create(DirExtract)
  }
  saveRDS(NewTable, paste0(DirExtract, NomeTabelaAtt, ".RDS"))
  rm(DirExtract, NewTable)
}

# Remove tabela temporária de dados
if(dbExistsTable(connec, "update_temp")) dbRemoveTable(connec, "update_temp")
dbExistsTable(connec, "update_temp") # checa extinção de tabela

# Desliga conexão
dbDisconnect(connec)

# Limpa memória
rm(connec, isTest, NomeTabelaAtt)
rm(DeleteRow, OldData)
ls()

# Fim ####
