# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: com base nos dados extraídos da Receita Federal, já passada a 
# de identificação das OSC (com o find_OSC) e determinação da área de atuação,
# extrair as principais tabelas do Mapa das Organizações Da Sociedade Civil:

# tb_osc
# tb_dados_gerais
# tb_contatos
# tb_localizacao
# tb_areas_atuacao

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# DB_OSC  (objeto na memódia ou arquivo em "{diretorio_att}intermediate_files")
# definicoes 
# diretorio_att
# processos_att_atual
# id_presente_att
# Caso definicoes$att_teste == FALSE:
## conexao_mosc 
## tb_backups_files
## tb_processos_atualizacao


## Funções auxiliares:
# "src/generalFunctions/postCon.R"
# "src/generalFunctions/agora.R"

## Outputs:
# (nenhum)

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

Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização


# Fim ####