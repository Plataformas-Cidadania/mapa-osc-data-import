# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: criar tabelas no banco de dados MOSC (portal_osc/osc) para controlar
# as atualizações dos dados

# Esse script deve ser executado apenas uma vez em cada banco

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

# Setup ####
## Inputs:
# credenciais_mosc (ver definição abaixo) 

## Funções auxiliares:
# "src/generalFunctions/postCon.R"

## Outputs:
# tb_controle_atualizacao (criação de tabela no banco de dados portal_osc)
# tb_processos_atualizacao (criação de tabela no banco de dados portal_osc)
# tb_backups_files (criação de tabela no banco de dados portal_osc)

# bibliotecas necessárias:
library(magrittr)
library(tidyverse)
library(lubridate)
library(assert_that)
library(DBI)
library(RODBC)
library(RPostgres)

# Conecta ao banco de dados, se necessário:
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))) {
  
  # Credenciais de acesso ao banco de dados:
  # credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"
  credenciais_mosc <- "keys/psql12-prod_key3.json"
  
  # Função para facilitar a conexão com os bancos de dados PostgreSQL:
  source("src/generalFunctions/postCon.R") 
  
  # Concecta aos bancos de dados do MOSC:
  conexao_mosc <- postCon(credenciais_mosc, Con_options = "-c search_path=osc")
  
  rm(credenciais_mosc, postCon)
  
}

# Cria as tabela 'tb_controle_atualizacao' ####

# Tabela para controlar a atualização como um todo
tb_controle_atualizacao <- tibble(
  att_id = integer(0), # id da atualização
  cd_att_ref = character(0), # código da atualização para ser usado em outros campos
  tx_att_situacao = character(0), # Se a atualização está iniciada ou finalizada
  dt_att_ref = as.Date(integer(0)), # data de referência da atualização (data dos dados originais)
  dt_att_inicio = as_datetime(integer(0)), # data de início da atualização
  dt_att_fim = as_datetime(integer(0)), # data do fim da atualiação
  .rows = 0
)

# TOMAR CUIDADO PARA NÃO DELETAR TABELA EXISTENTE!

assert_that(!dbExistsTable(conexao_mosc, "tb_controle_atualizacao"))
# dbRemoveTable(conexao_mosc, "tb_controle_atualizacao")

if(!dbExistsTable(conexao_mosc, "tb_controle_atualizacao")) {
  dbWriteTable(conexao_mosc, "tb_controle_atualizacao", tb_controle_atualizacao)
}

# Cria as tabela 'tb_processos_atualizacao' ####

# Tabela para registrar cada processo da atualiação
tb_processos_atualizacao <- tibble(
  att_id = integer(0), # id da atualização
  processo_id = integer(0), # id do processo
  tx_processo_nome = character(0), # nome do processo
  bo_processo_att_completo = logical(0) , # indica se o processo está concluído
  dt_processo_att_inicio = as_datetime(integer(0)), # data de início do processo
  dt_processo_att_fim = as_datetime(integer(0)), # data de fim do processo
  nr_processo_att_controle = integer(0), # processo_id e ifelse(processo_at_completo, 1, 0)
  
  .rows = 0
)

# TOMAR CUIDADO PARA NÃO DELETAR TABELA EXISTENTE!

assert_that(!dbExistsTable(conexao_mosc, "tb_processos_atualizacao"))
# dbRemoveTable(conexao_mosc, "tb_processos_atualizacao")

if(!dbExistsTable(conexao_mosc, "tb_processos_atualizacao")) {
  dbWriteTable(conexao_mosc, "tb_processos_atualizacao", tb_processos_atualizacao)
}

# Cria as tabela 'tb_backups_files' ####

# Tabela para registrar cada processo da atualiação
tb_backups_files <- tibble(
  file_id = integer(0), # id do arquivo gerado
  att_id = integer(0), # id da atualização
  processo_id = integer(0), # id do processo
  tx_file_folder = character(0), # diretório onde se encontra o arquivo
  tx_file_name = character(0), # nome do arquivo
  nr_file_size_mb = numeric(0), # tamanho do arquivo
  
  .rows = 0
)

# TOMAR CUIDADO PARA NÃO DELETAR TABELA EXISTENTE!

assert_that(!dbExistsTable(conexao_mosc, "tb_backups_files"))
# dbRemoveTable(conexao_mosc, "tb_backups_files")

if(!dbExistsTable(conexao_mosc, "tb_backups_files")) {
  dbWriteTable(conexao_mosc, "tb_backups_files", tb_backups_files)
}

# Verifica a criação das tabelas e finaliza a rotina ####

Tables <- dbListTables(conexao_mosc)
Tables

# Verifica se todas as tabelas foram inseridas:
c("tb_controle_atualizacao", "tb_backups_files", "tb_processos_atualizacao") %in% 
  Tables
  
# Desconecta da base
dbDisconnect(conexao_mosc)

rm(Tables, conexao_mosc)
rm(tb_controle_atualizacao, tb_backups_files, tb_processos_atualizacao)
# Fim ####
