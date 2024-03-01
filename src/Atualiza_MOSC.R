# Scrip para controlar as funções de atualização MOSC

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-03-01

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa a chave secreta do código
keys <- jsonlite::read_json("keys/rais_2019_key.json")


# Verifica se pode condenar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = "mapa_osc",
                            host = "localhost",
                            port = "5432",
                            user = keys$username, 
                            password = keys$password,
                            # user = "postgres", 
                            # password = "I&E$hx*UxY4Akzph", 
                            options="-c search_path=osc")

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = "mapa_osc",
                    host = "localhost",
                    port = "5432",
                    user = keys$username, 
                    password = keys$password,
                    # user = "postgres", 
                    # password = "I&E$hx*UxY4Akzph", 
                    options="-c search_path=osc")

# Verifica a coneção com a base
dbIsValid(connec)

rm(keys)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza dados no Postgree do MOSC ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função de atualização de dados:
source("src/BDConnection.R")

ArquivosAtualizacao <- list(tb_osc = "backup_files/2023_01/output_files/tb_osc.RDS", 
                           tb_dados_gerais = "backup_files/2023_01/output_files/tb_dados_gerais.RDS",
                           # tb_area_atuacao = "backup_files/2023_01/output_files/tb_area_atuacao.RDS",
                           tb_contato = "backup_files/2023_01/output_files/tb_contato.RDS")

for (i in seq_along(ArquivosAtualizacao)) {
  # i <- 4
  
  # Garante que a função existe
  assert_that(exists("AtualizaDados"))
  assert_that(class(AtualizaDados) == "function")
  
  message("Inserindo dados da tabela '", names(ArquivosAtualizacao)[i], "'")
  
  DadosNovos <- readRDS(ArquivosAtualizacao[[i]])
  
  AtualizaDados(Conexao = connec, 
                DadosNovos = DadosNovos, 
                Chave = "id_osc", 
                Table_NameAntigo = names(ArquivosAtualizacao)[i], 
                verbose = FALSE)
  }



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finalização da rotina ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Desconecta da base
dbDisconnect(connec)

rm(connec, Tables)


# Fim ####
