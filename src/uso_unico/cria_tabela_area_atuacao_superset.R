# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: criar uma tabela de frequência das áreas de atuação das OSC
# usando SQL para poder ser incorporada ao SuperSet MOSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-10-22


# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres) 



# Conecta à base de dados MOCS:
source("src/generalFunctions/postCon.R") 
conexao_rfb <- postCon("keys/psql12-homolog_keySUPER.json", # Arquivo JSON com as chave de acesso ao banco de dados MOSC
                       Con_options = "-c search_path=osc")

# Solução baseada em: https://stackoverflow.com/questions/29020065/conditional-sql-count

tables <- dbListTables(conexao_rfb)
tables

query <- glue("SELECT * FROM tb_area_atuacao 
                LIMIT 10000;")

query

Busca <- dbGetQuery(conexao_rfb, query)

names(Busca)

queryareas <- glue(
"select cd_area_atuacao      
	, count(*) FILTER (WHERE cd_area_atuacao = 3) AS cultura_recreacao      
	, count(*) FILTER (WHERE cd_area_atuacao = 4) AS educacao_pesquisa      
from   tb_area_atuacao 
GROUP  BY 1;"
  )

queryareas <- glue(
  "select cd_area_atuacao      
	, count(*)
from   tb_area_atuacao 
GROUP  BY cd_area_atuacao;"
)

queryareas

tbfreq_areas <- dbGetQuery(conexao_rfb, queryareas)

tbfreq_areas


querysubareas <- glue(
  "select cd_subarea_atuacao      
	, count(*)
from   tb_area_atuacao 
GROUP  BY cd_subarea_atuacao;"
)

querysubareas

tbfreq_subareas <- dbGetQuery(conexao_rfb, querysubareas)

tbfreq_subareas
