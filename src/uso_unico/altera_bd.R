# Objetivo do Script: script para facilitar alterações pontuais no banco de dados

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2025-05-08

library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Concecta aos bancos de dados do MOSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

tables

# rm(tables)

col_names <- dbGetQuery(
  conexao_mosc, 
  glue(
    "SELECT *",
    "FROM information_schema.columns ",
    "WHERE table_name   = 'tb_controle_atualizacao'",
    ";"
  )
)

col_names$column_name

# Baixa as informações das tabelas
old_data <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM tb_controle_atualizacao",
           " LIMIT 500",
           ";")
  )
)

View(old_data)
class(old_data)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Alterar uma linha específica no banco de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
#                      " SET tx_att_situacao = 'finalizada' \n",
#                      " WHERE att_id = 3",
#                      ";")
# 
# query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
#                      " SET dt_att_fim = '2024-11-21 17:07'::timestamp \n",
#                      " WHERE att_id = 1",
#                      ";")
# 
# query_AltRow
# 
# dbExecute(conexao_mosc, query_AltRow)

# rm(query_AltRow)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria uma coluna no banco de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# query_AddCol <- glue("ALTER TABLE tb_dados_gerais \n",
#                        " ADD COLUMN cd_cnae_secundaria TEXT",
#                        ";")
# 
# query_AddCol
# 
# dbExecute(conexao_mosc, query_AddCol)
# 
# rm(query_AddCol)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Exclui uma coluna no banco de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# query_DropCol <- glue("ALTER TABLE tb_controle_atualizacao \n",
#                         " DROP COLUMN IF EXISTS tc_att_comentarios;")
# 
# query_DropCol
# 
# dbExecute(conexao_mosc, query_DropCol)
# 
# rm(query_DropCol)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Exclui uma linha no banco de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# query_DropRow <- glue("DELETE FROM tb_controle_atualizacao \n",
#                      " WHERE tx_att_situacao = 'Iniciada' ",
#                      ";")
# 
# query_DropRow
# 
# dbExecute(conexao_mosc, query_DropRow)
# 
# rm(query_DropRow)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finaliza a rotina ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# De forma profilática, desconecta do banco de dados
dbDisconnect(conexao_mosc)
rm(conexao_mosc)

# Limpa memória
rm(tables, col_names)
ls()
gc()
