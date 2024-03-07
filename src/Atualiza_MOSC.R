# Scrip para controlar as funções de atualização MOSC

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-03-01

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa a chave secreta do código
assert_that(file.exists("keys/rais_2019_key2.json"))
keys <- jsonlite::read_json("keys/rais_2019_key2.json")


# Verifica se pode condenar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = keys$dbname,
                            host = keys$host,
                            port = keys$port,
                            user = keys$username, 
                            password = keys$password,
                            options="-c search_path=osc")

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = keys$dbname,
                    host = keys$host,
                    port = keys$port,
                    user = keys$username, 
                    password = keys$password,
                    options="-c search_path=osc")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Testes ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Testa extrair dados do banco
Teste <- dbGetQuery(connec, paste0("SELECT * FROM tb_osc",
                                   " LIMIT 500", 
                                   ";"))

# Testa inserir uma nova tabela:
if(dbExistsTable(connec, "teste")) dbRemoveTable(connec, "teste")
dbWriteTable(connec, "teste", Teste)

# Verifica se a tabela fo inserida corretamente
assert_that(dbExistsTable(connec, "teste"))
Tables <- dbListTables(connec)
assert_that("teste" %in% Tables)

Teste_verific <- dbGetQuery(connec, "SELECT * FROM teste;")

assert_that(all(names(Teste) == names(Teste_verific)))
assert_that(all(dim(Teste) == dim(Teste_verific)))
# head(Teste)
# head(Teste_verific)
# tail(Teste)
# tail(Teste_verific)

# Verifica se podemos deletar tabela:
if(dbExistsTable(connec, "teste")) {
  dbRemoveTable(connec, "teste")
}
assert_that(!dbExistsTable(connec, "teste"))
rm(Teste, Teste_verific)

# Verifica se os arquivos existem:
assert_that(file.exists("backup_files/2023_01/output_files/tb_osc.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_dados_gerais.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_area_atuacao.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_contato.RDS"))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega função de atualização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função de atualização de dados:
source("src/BDConnection.R")

# Garante que a função existe
assert_that(exists("AtualizaDados"))
assert_that(class(AtualizaDados) == "function")



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_osc: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_osc_teste'")

# Carrega dados RDS:
tb_osc_teste <- readRDS("backup_files/2023_01/output_files/tb_osc.RDS")

# Corrige tipo de dado para "cd_identificador_osc"
tb_osc_teste[["cd_identificador_osc"]] <- as.numeric(tb_osc_teste[["cd_identificador_osc"]])

# Por algum motivo, "tx_apelido_osc" está contrangido como valor único
# e, para solucionar isso, foi inteiro determinado como NA
tb_osc_teste[["tx_apelido_osc"]] <- NA_character_
tb_osc_teste[["ft_apelido_osc"]] <- NA_character_

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_osc_teste, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_osc_teste", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_osc_teste)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_dados_gerais: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_dados_gerais_teste'")

# Carrega dados RDS:
tb_dados_gerais_teste <- readRDS("backup_files/2023_01/output_files/tb_dados_gerais.RDS")

# Corrige tipos de dado
tb_dados_gerais_teste[["cd_natureza_juridica_osc"]] <- as.numeric(tb_dados_gerais_teste[["cd_natureza_juridica_osc"]])
tb_dados_gerais_teste[["dt_fundacao_osc"]] <- as_date(tb_dados_gerais_teste[["dt_fundacao_osc"]])

# Esta variável tem um interpretação diferente no banco de dados antigo e novo (investigar!)
tb_dados_gerais_teste[["dt_ano_cadastro_cnpj"]] <- NA_Date_

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_dados_gerais_teste, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_dados_gerais_teste", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_dados_gerais_teste)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_contato: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_contato_teste'")

# Carrega dados RDS:
tb_contato_teste <- readRDS("backup_files/2023_01/output_files/tb_contato.RDS")

# Corrige tipos de dado
tb_contato_teste[["cd_natureza_juridica_osc"]] <- as.numeric(tb_contato_teste[["cd_natureza_juridica_osc"]])


# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_contato_teste, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_contato_teste", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_contato_teste)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_area_atuacao: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Estou aqui!!! ####

# message("Inserindo dados da tabela 'tb_area_atuacao_teste'")
# 
# # Carrega dados RDS:
# tb_area_atuacao_teste <- readRDS("backup_files/2023_01/output_files/tb_area_atuacao.RDS")
# 
# # Corrige tipos de dado
# tb_area_atuacao_teste[["cd_natureza_juridica_osc"]] <- as.numeric(tb_area_atuacao_teste[["cd_natureza_juridica_osc"]])
# 
# 
# # Executa atualização
# Atualizacao <- AtualizaDados(Conexao = connec, 
#                              DadosNovos = tb_area_atuacao_teste, 
#                              Chave = "id_osc", 
#                              Table_NameAntigo = "tb_area_atuacao_teste", 
#                              verbose = TRUE, 
#                              samples = TRUE)
# 
# assert_that(Atualizacao)
# rm(Atualizacao, tb_area_atuacao_teste)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finalização da rotina ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Desconecta da base
dbDisconnect(connec)

rm(connec, Tables)


# Fim ####
