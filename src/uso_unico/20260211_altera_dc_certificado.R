# Objetivo do Script: inserir CNEAS como certificado no MOSCm 
# tabela syst.dc_certificado

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2026-02-11

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
conexao_mosc <- postCon(keys
                        , Con_options = "-c search_path=syst"
                        )

dbIsValid(conexao_mosc)

rm(postCon, keys)


# Informações sobre o banco de dados  
tables <- dbListTables(conexao_mosc)

tables

# rm(tables)


# Verifica como a tabela está atualmente
old_data <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM dc_certificado",
           " LIMIT 500",
           ";")
  )
)

old_data

# Cria linha com os novos dados
new_row <- tibble(.rows = 1) %>% 
  mutate(
    cd_certificado = 10L,
    tx_nome_certificado = "CNEAS"
  )

new_row


# Adiciona linha
dbAppendTable(conexao_mosc, "dc_certificado", new_row)


# Verifica se deu certo a alteração:
check_data <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM dc_certificado",
           " LIMIT 500",
           ";")
  )
)

check_data


# Finaliza a Rotina
rm(check_data, tables, new_row, old_data)
dbDisconnect(conexao_mosc)
rm(conexao_mosc)

ls()
gc()

# Fim ####