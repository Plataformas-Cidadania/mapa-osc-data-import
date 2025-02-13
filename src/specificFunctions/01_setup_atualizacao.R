# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: carrega bibliotecas e funções usadas em toda a atualização, 
# bem como coloca definições que raramente mudam.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

## Inputs:
# definicoes 

## Funções auxiliares:
# "src/generalFunctions/agora.R"

## Outputs:
# definicoes (atualização)

# bibliotecas necessárias:
# (ver abaixo)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Iniciando Atualização...")
message("Carregando bibliotecas...")
# Dar um tempo apenas para o usuário ler as mensagens da atualização
Sys.sleep(2)

# Pacotes de programação e manipulação de dados
library(magrittr)
library(tidyverse) 
library(assertthat) 

# Pacotes de leitura de dados externos
library(data.table)
library(readxl)
library(jsonlite) 

# Manipulação de datas
library(lubridate) 

# Manipulação de Textos
library(stringr) 
library(glue) 

# Pacotes de Bancos de dados (ex: PostgreSQL)
library(DBI)
library(RODBC)
library(RPostgres) 
library(dbplyr) 

assert_that(file.exists("src/generalFunctions/agora.R"))
assert_that(file.exists("src/specificFunctions/atualiza_processos_att.R"))

# Carrega essa função facilitadora da marcação do tempo:
source("src/generalFunctions/agora.R")

# Atualiza processos de atualização
source("src/specificFunctions/atualiza_processos_att.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Mais definições importantes ! ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Aqui estão definições imporantes que existe pouca chance de mudar no 
# curto e médio prazo:

# Definição do fuso horário:
Sys.setenv(TZ = "America/Sao_Paulo")
assert_that(Sys.timezone() == "America/Sao_Paulo")

# Caminho para o arquivo mestre de atualização (esse arquivo aqui)
# mudar se a localização ou o nome do arquivo mudar.
definicoes$path_rscript_att_mosc <- "src/atualiza_dados_OSC.R"

# Diretório onde está localizado os arquivos de backup da atualização:
definicoes$dir_backup_files <- "backup_files/"

Sys.sleep(1)
message("Setup concluído com sucesso!")
Sys.sleep(2)

# Fim ####
