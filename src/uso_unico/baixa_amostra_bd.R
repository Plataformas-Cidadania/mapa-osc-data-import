# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: baixar a amostra de uma tabela do do MOSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-09-17

# Setup ####

# bibliotecas necessárias:
library(magrittr)
library(tidyverse)
library(lubridate)
library(assert_that)
library(glue)
library(DBI)
library(RODBC)
library(RPostgres)

credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"

# Função para facilitar a conexão com os bancos de dados PostgreSQL:
source("src/generalFunctions/postCon.R") 

# Concecta aos bancos de dados do MOSC:
conexao_mosc <- postCon(credenciais_mosc, Con_options = "-c search_path=osc")
rm(credenciais_mosc, postCon)

# Lista de tabelas do banco:
Tables <- dbListTables(conexao_mosc)
Tables

tabela_amostra <- "tb_localizacao"

amostra <- dbGetQuery(conexao_mosc, 
                            glue("SELECT * FROM {tabela_amostra}",
                                 # " LIMIT 500", 
                                 ";"))

# Salva Amostra:
saveRDS(amostra, "data/temp/tb_localizacao_old.RDS")

# De forma profilática, desconecta do banco de dados
dbDisconnect(conexao_mosc)
rm(amostra, tabela_amostra, Tables)
rm(conexao_mosc)

# Fim ####