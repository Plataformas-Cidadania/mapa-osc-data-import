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

# Carrega essa função facilitadora da marcação do tempo:
assert_that(file.exists("src/generalFunctions/agora.R"))
source("src/generalFunctions/agora.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Mais definições importantes ! ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Aqui estão definições imporantes que existe pouca chance de mudar no 
# curto e médio prazo:

# Definição do fuso horário:
Sys.setenv(TZ = "America/Sao_Paulo")
assert_that(Sys.timezone() == "America/Sao_Paulo")

# Modelo de dados da da Receita federal:
# (somente atualizar se mudar o modelo de dados)
definicoes$tabela_empresas_rfb <- "empresas"
definicoes$tabela_estabelecimentos_rfb <- "estabelecimento"
definicoes$campo_rfb_cnpj <- "cnpj_basico" # campo CNPJ da Receita
definicoes$campo_rfb_natureza_juridica <- "natureza_juridica" # nome do campo da natureza jurídica
definicoes$campo_rfb_cnae_principal <- "cnae_fiscal"
definicoes$campo_rfb_razao_social <- "razao_social"

# naturesas jurídicas não lucrativas
definicoes$natjur_nao_lucarivo <- c(3069, 3220, 3301, 3999) 


# Caminho para o arquivo mestre de atualização (esse arquivo aqui)
# mudar se a localização ou o nome do arquivo mudar.
definicoes$path_rscript_att_mosc <- "src/OSC-v2023.R"

# Diretório onde está localizado os arquivos de backup da atualização:
definicoes$dir_backup_files <- "backup_files/"

Sys.sleep(1)
message("Setup concluído com sucesso!")
Sys.sleep(2)

# Fim ####