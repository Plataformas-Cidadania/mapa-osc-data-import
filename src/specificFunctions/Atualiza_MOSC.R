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
KeyFile <- "keys/psql12-homolog_keySUPER.json"
assert_that(file.exists(KeyFile))
keys <- jsonlite::read_json(KeyFile)


# Verifica se pode conectar
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

rm(keys, TestConexao, KeyFile)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Testes ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Testa extrair dados do banco
Teste <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_osc",
                               " LIMIT 500", 
                               ";")))
assert_that(!is.error(Teste))

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

# Verifica PostGIS
TesteFuncGeo <- try(dbGetQuery(connec, "SELECT public.ST_MakePoint(-50.3482090039996,-20.7619611619996);"))
assert_that(!is.error(TesteFuncGeo), 
            msg = "Função 'public.ST_MakePoint' não encontrada")
rm(TesteFuncGeo)

# Verifica se os arquivos existem:
assert_that(file.exists("backup_files/2023_01/output_files/tb_osc.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_dados_gerais.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_area_atuacao.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_contato.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_localizacao.RDS"))
assert_that(file.exists("backup_files/2023_01/output_files/tb_relacoes_trabalho.RDS"))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega função de atualização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função de atualização de dados:
source("src/BDConnection.R")

# Garante que a função existe
assert_that(exists("AtualizaDados"))
assert_that(is.function(AtualizaDados))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_osc: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_osc'")

# Deixa, a princípio, a variável bo_osc_ativa como FALSE, voltando para TRUE na presença dos dados novos
dbExecute(connec, paste0("UPDATE tb_osc", "\n",
                         " SET bo_osc_ativa = FALSE",
                         ";"))

# Carrega dados RDS:
tb_osc <- readRDS("backup_files/2023_01/output_files/tb_osc.RDS")

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_osc, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_osc", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_osc)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_dados_gerais: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_dados_gerais'")

# Carrega dados RDS:
tb_dados_gerais <- readRDS("backup_files/2023_01/output_files/tb_dados_gerais.RDS")

# Corrige tipos de dado
tb_dados_gerais[["cd_natureza_juridica_osc"]] <- as.numeric(tb_dados_gerais[["cd_natureza_juridica_osc"]])
tb_dados_gerais[["dt_fundacao_osc"]] <- as_date(tb_dados_gerais[["dt_fundacao_osc"]])

# Esta variável tem um interpretação diferente no banco de dados antigo e novo (investigar!)
# tb_dados_gerais[["dt_ano_cadastro_cnpj"]] <- NA_Date_
tb_dados_gerais[["dt_ano_cadastro_cnpj"]] <- as_date(tb_dados_gerais[["dt_ano_cadastro_cnpj"]])

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_dados_gerais, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_dados_gerais", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_dados_gerais)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_contato: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_contato'")

# Carrega dados RDS:
tb_contato <- readRDS("backup_files/2023_01/output_files/tb_contato.RDS")

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec, 
                             DadosNovos = tb_contato, 
                             Chave = "id_osc", 
                             Table_NameAntigo = "tb_contato", 
                             verbose = TRUE, 
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_contato)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_localizacao: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_localizacao'")

# Carrega dados RDS:
tb_localizacao <- readRDS("backup_files/2023_01/output_files/tb_localizacao.RDS")

# Arrumar a classe das variáveis
tb_localizacao[["dt_geocodificacao"]] <- as_date(tb_localizacao[["dt_geocodificacao"]])
tb_localizacao[["cd_fonte_geocodificacao"]] <- as.integer(tb_localizacao[["cd_fonte_geocodificacao"]])
tb_localizacao[["tx_bairro_encontrado"]] <- as.character(tb_localizacao[["tx_bairro_encontrado"]])
tb_localizacao[["ft_bairro_encontrado"]] <- as.character(tb_localizacao[["ft_bairro_encontrado"]])

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec,
                             DadosNovos = tb_localizacao,
                             Chave = "id_osc",
                             Table_NameAntigo = "tb_localizacao",
                             GeoVar = c("geo_localizacao"),
                             verbose = TRUE,
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_localizacao)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_area_atuacao: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_area_atuacao'")

# Carrega dados RDS:
tb_area_atuacao <- readRDS("backup_files/2023_01/output_files/tb_area_atuacao.RDS")

# Corrige tipos de dado
tb_area_atuacao[["id_osc"]] <- as.integer(tb_area_atuacao[["id_osc"]])


# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec,
                             DadosNovos = tb_area_atuacao,
                             Chave = "id_area_atuacao",
                             Table_NameAntigo = "tb_area_atuacao",
                             verbose = TRUE,
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_area_atuacao)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_relacoes_trabalho (RAIS): ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Estou aqui!!! ####

message("Inserindo dados da tabela 'tb_relacoes_trabalho'")

# Carrega dados RDS:
tb_relacoes_trabalho <- readRDS("backup_files/2023_01/output_files/tb_relacoes_trabalho.RDS")

# Corrige tipos de dado
# tb_relacoes_trabalho[["nr_trabalhadores_voluntarios"]] <- as.integer(tb_relacoes_trabalho[["nr_trabalhadores_voluntarios"]])
# tb_relacoes_trabalho[["ft_trabalhadores_voluntarios"]] <- as.character(tb_relacoes_trabalho[["ft_trabalhadores_voluntarios"]])

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec,
                             DadosNovos = tb_relacoes_trabalho,
                             Chave = "id_osc",
                             Table_NameAntigo = "tb_relacoes_trabalho",
                             verbose = TRUE,
                             samples = TRUE)

assert_that(Atualizacao)
rm(Atualizacao, tb_relacoes_trabalho)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza dos views ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

assert_that(file.exists("tab_auxiliares/31_refresh_views_mat.sql"))

refresh_views <- read_lines("tab_auxiliares/31_refresh_views_mat.sql")

for (i in seq_along(refresh_views)) {
  message(refresh_views[i])
  dbExecute(connec, refresh_views[i])
}
rm(i, refresh_views)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finalização da rotina ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Desconecta da base
dbDisconnect(connec)

rm(connec, Tables)
rm(AtualizaDados)

# Fim ####
