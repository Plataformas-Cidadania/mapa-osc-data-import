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
                            dbname = "portal_osc2",
                            host = "psql12-dev",
                            port = "5432",
                            # user = keys$username, 
                            # password = keys$password,
                            user = "thiago.ramos", 
                            password = "123456", 
                            options="-c search_path=osc")

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = "portal_osc2",
                    host = "psql12-dev",
                    port = "5432",
                    # user = keys$username, 
                    # password = keys$password,
                    user = "thiago.ramos", 
                    password = "123456",
                    options="-c search_path=osc")

# Verifica a coneção com a base
dbIsValid(connec)

rm(keys)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Testes ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Testa extrair dados do banco
Teste <- dbGetQuery(connec, paste0("SELECT * FROM tb_osc",
                                   " LIMIT 500", 
                                   ";"))

# Testa inserir uma nova tabela:
dbWriteTable(connec, "teste", Teste)

# Verifica se a tabela fo inserida corretamente
dbExistsTable(connec, "teste")
Tables <- dbListTables(connec)
Tables

Teste_verific <- dbGetQuery(connec, "SELECT * FROM teste;")

names(Teste)
names(Teste_verific)
dim(Teste)
dim(Teste_verific)
head(Teste)
head(Teste_verific)
tail(Teste)
tail(Teste_verific)

# Verifica se podemos deletar tabela:
if(dbExistsTable(connec, "deletedata")) {
  dbRemoveTable(connec, "deletedata")
}
dbExistsTable(connec, "teste")


# Verifica se os arquivos existem:
file.exists("backup_files/2023_01/output_files/tb_osc.RDS")
file.exists("backup_files/2023_01/output_files/tb_dados_gerais.RDS")
file.exists("backup_files/2023_01/output_files/tb_area_atuacao.RDS")
file.exists("backup_files/2023_01/output_files/tb_contato.RDS")


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
  # i <- 2
  
  # Garante que a função existe
  assert_that(exists("AtualizaDados"))
  assert_that(class(AtualizaDados) == "function")
  
  message("Inserindo dados da tabela '", names(ArquivosAtualizacao)[i], "'")
  
  DadosNovos <- readRDS(ArquivosAtualizacao[[i]])
  
  # Ajustas para "tb_osc"
  if(names(ArquivosAtualizacao)[i] == "tb_osc") {
    
    # Corrige tipo de dado para "cd_identificador_osc"
    DadosNovos[["cd_identificador_osc"]] <- as.numeric(DadosNovos[["cd_identificador_osc"]])
    
    # Por algum motivo, "tx_apelido_osc" está contrangido como valor único
    DadosNovos[["tx_apelido_osc"]] <- NA
  }
  
  # Ajustas para "tb_dados_gerais"
  if(names(ArquivosAtualizacao)[i] == "tb_dados_gerais") {
    
    # Corrige tipos de dado
    DadosNovos[["cd_natureza_juridica_osc"]] <- as.numeric(DadosNovos[["cd_natureza_juridica_osc"]])
    DadosNovos[["dt_fundacao_osc"]] <- as_date(DadosNovos[["dt_fundacao_osc"]])
    DadosNovos[["dt_ano_cadastro_cnpj"]] <- as_date(DadosNovos[["dt_ano_cadastro_cnpj"]])
  }
  
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
