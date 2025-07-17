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




# Uma série de procedmentos para evitar que se atualize o banco de dados errado:

assert_that(definicoes$Banco_Atualização[1] %in% c("Homologação", "Produção"), 
            msg = "Valor de 'definicoes$Banco_Atualização' não permitido")

# Pede para o usuário digitar o nome do banco que quer atualizar:
ConfirmacaoBanco <- readline(
  prompt = glue(
    "Digite o banco que vai ser atualizado (opção atual: '", 
    definicoes$Banco_Atualização[1],
    "') : "
    )
  )

assert_that(ConfirmacaoBanco == definicoes$Banco_Atualização[1], 
            msg = "Banco digitado não confere com 'definicoes$Banco_Atualização'") 

# Arquivo com as credenciais do banco que se quer atualizar:

if (definicoes$Banco_Atualização[1] == "Homologação") {
  definicoes$credenciais_mosc <- "keys/psql12-homolog_key.json" # Banco de homologação
}

if (definicoes$Banco_Atualização[1] == "Produção") {
  definicoes$credenciais_mosc <- "keys/psql12-usr_manutencao_mapa.json" # acesso completo ao banco de produção
  # definicoes$credenciais_mosc <- "keys/psql12-prod_key3.json" # acesso limitado ao banco de proução
}

rm(ConfirmacaoBanco)

assert_that(file.exists(definicoes$credenciais_mosc), 
            msg = "Arquivo de credenciais MOSC não encontrado")


Sys.sleep(1)
message("Setup concluído com sucesso!")
Sys.sleep(2)

# Fim ####
