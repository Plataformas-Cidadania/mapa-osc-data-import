

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


# Conecta à nase de dados da Receita Federal:
source("src/generalFunctions/postCon.R") 

read_json("keys/rais_2019_MuriloJunqueira.json")

conexao_rfb <- postCon("keys/rais_2019_MuriloJunqueira.json", 
                       Con_options = "-c search_path=rfb_2023")


if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")


tables <- dbListTables(conexao_rfb)
tables

query <- glue("SELECT * FROM tb_rfb_estabelecimentos 
                ORDER BY data_de_inicio_atividade DESC
                LIMIT 100;")

query

Busca <- dbGetQuery(conexao_rfb, query)

max(Busca$data_de_inicio_atividade, na.rm = TRUE)

"20230812"



conexao_rfb <- postCon("keys/rais_2019_MuriloJunqueira.json", 
                       Con_options = "-c search_path=rfb_2024")


if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")


tables <- dbListTables(conexao_rfb)
tables


query <- glue("SELECT * FROM estabelecimento 
                LIMIT 100;")

Busca <- dbGetQuery(conexao_rfb, query)

names(Busca)

query <- glue("SELECT * FROM estabelecimento 
                ORDER BY data_inicio_atividades DESC
                LIMIT 1;")

query

Busca <- dbGetQuery(conexao_rfb, query)


max(Busca$data_inicio_atividades, na.rm = TRUE)


"20240413"
