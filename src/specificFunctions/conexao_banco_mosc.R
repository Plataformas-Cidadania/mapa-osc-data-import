# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: conectar ao banco de dados do Mapa das Organizações da 
# Sociedade Civil (MOSC).

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes 

## Funções auxiliares:
# "src/generalFunctions/postCon.R"
# "src/generalFunctions/agora.R"

## Outputs:
# conexao_mosc

# bibliotecas necessárias:
library(magrittr)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conexão com os bancos de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função para facilitar a conexão com os bancos de dados PostgreSQL:
assert_that(file.exists("src/generalFunctions/postCon.R"))
source("src/generalFunctions/postCon.R") 
assert_that(is.function(postCon))

message(agora(), "  Conectando aos Bancos de Dados MOSC")
Sys.sleep(1) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Primeiro vamos checar se conseguimos nos conectar ao banco de dados:
assert_that("credenciais_mosc" %in% names(definicoes), 
            msg = "O objeto 'credenciais_mosc', não foi carregado!")

# Concecta aos bancos de dados do MOSC:
conexao_mosc <- postCon(definicoes$credenciais_mosc, 
                        Con_options = "-c search_path=osc")

if(dbIsValid(conexao_mosc)) message("Conectado ao BD 'portal_osc'")

rm(postCon)

# Fim ####