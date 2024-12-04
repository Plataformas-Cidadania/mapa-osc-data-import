# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: criar as variáveis de data de fechamento das OSC e cnae 
# secundária

# Esse script deve ser executado apenas uma vez em cada banco

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-12-04

# Setup ####
## Inputs:
# credenciais_mosc (ver definição abaixo) 

## Funções auxiliares:
# "src/generalFunctions/postCon.R"

## Outputs:
# alterações em tb_dados_gerais

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
  credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"
  
  # Função para facilitar a conexão com os bancos de dados PostgreSQL:
  source("src/generalFunctions/postCon.R") 
  
  # Concecta aos bancos de dados do MOSC:
  conexao_mosc <- postCon(credenciais_mosc, Con_options = "-c search_path=osc")
  
  rm(credenciais_mosc, postCon)
  
}

# Cria coluna dt_fechamento_osc
query_AddCol <- paste0("ALTER TABLE tb_dados_gerais",
                       " ADD COLUMN dt_fechamento_osc TEXT",  
                       ";")

dbExecute(conexao_mosc, query_AddCol)


# Cria coluna nr_ano_fechamento_osc
query_AddCol <- paste0("ALTER TABLE tb_dados_gerais",
                       " ADD COLUMN nr_ano_fechamento_osc TEXT",  
                       ";")

dbExecute(conexao_mosc, query_AddCol)


# Cria coluna ft_fechamento_osc
query_AddCol <- paste0("ALTER TABLE tb_dados_gerais",
                       " ADD COLUMN ft_fechamento_osc TEXT",  
                       ";")

dbExecute(conexao_mosc, query_AddCol)


# Cria coluna cd_cnae_secundaria
query_AddCol <- paste0("ALTER TABLE tb_dados_gerais",
                       " ADD COLUMN cd_cnae_secundaria TEXT",  
                       ";")

dbExecute(conexao_mosc, query_AddCol)


# Verifica se as colunas foram criadas corretamente:

# Usando o arquivo do banco de dados
tb_dados_gerais <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_dados_gerais",
                                                   " LIMIT 500", 
                                                   ";"))

names(tb_dados_gerais)

# Desconecta da base
dbDisconnect(conexao_mosc)

rm(Tables, conexao_mosc)
rm(tb_controle_atualizacao, tb_backups_files, tb_processos_atualizacao)
# Fim ####
