# Scrip para testar modificações na tabela tb_area_atuacao

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-07-08

library(tidyverse)
library(data.table)
library(stringr)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres)


source("src/generalFunctions/postCon.R")
connec <- postCon("keys/psql12-homolog_keySUPER.json", 
                  Con_options = "-c search_path=osc")
rm(postCon)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega função de atualização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função de atualização de dados:
source("src/BDConnection.R")

# Garante que a função existe
assert_that(exists("AtualizaDados"))
assert_that(is.function(AtualizaDados))

Tables <- dbListTables(connec)
assert_that("tb_area_atuacao_teste" %in% Tables)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_area_atuacao_teste: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_area_atuacao_teste'")

# Carrega dados RDS:
tb_area_atuacao <- readRDS("backup_files/2023_01/output_files/tb_area_atuacao_teste2.RDS")

# Corrige tipos de dado
tb_area_atuacao[["id_osc"]] <- as.integer(tb_area_atuacao[["id_osc"]])


# Executa atualização
Atualizacao <- AtualizaDados(Conexao = connec,
                             DadosNovos = tb_area_atuacao,
                             Chave = "id_area_atuacao",
                             Table_NameAntigo = "tb_area_atuacao_teste",
                             verbose = TRUE,
                             samples = TRUE)

assert_that(Atualizacao)


# Verifica se deu tudo certo
teste <- try(dbGetQuery(connec, 
                           paste0("SELECT * FROM tb_area_atuacao_teste",
                                  # " LIMIT 500", 
                                  ";")))

teste %>% 
  group_by(id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
  mutate(n = row_number()) %>% 
  ungroup() %>% 
  select(n) %>% 
  unlist() %>% as.integer() %>% 
  table()

table(teste$ft_area_atuacao)


saveRDS(teste, "data/temp/tb_area_atuacao_Atualizada_teste.RDS")


# Desliga conexão
dbDisconnect(connec)

# Limpa memória
rm(Atualizacao, AtualizaDados, connec)
rm(Tables, tb_area_atuacao, teste)
ls()





