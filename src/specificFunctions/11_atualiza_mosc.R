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

# Verifica se os arquivos existem:

assert_that(file.exists(glue("{diretorio_att}output_files/tb_osc.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_dados_gerais.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_area_atuacao.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_contato.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_localizacao.RDS")))


# Atualiza controle de processos (tb_processos_atualizacao)  
if(!definicoes$att_teste) atualiza_processos_att(
  TipoAtt = "inicio", 
  id_att = id_presente_att, 
  id_processo = 4, 
  processo_nome = "Update banco de dados MOSC")

# tb_processos_atualizacao

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Inicia a conexão com o bd portal_osc (se necessário):
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))){
  source("src/specificFunctions/conexao_banco_mosc.R")
}


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
dbExecute(conexao_mosc, paste0("UPDATE tb_osc", "\n",
                               " SET bo_osc_ativa = FALSE",
                               ";"))

# Carrega dados RDS:
tb_osc <- readRDS(glue("{diretorio_att}output_files/tb_osc.RDS"))

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = conexao_mosc, 
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
tb_dados_gerais <- readRDS(glue("{diretorio_att}output_files/tb_dados_gerais.RDS"))

# Corrige tipos de dado
tb_dados_gerais[["cd_natureza_juridica_osc"]] <- as.numeric(tb_dados_gerais[["cd_natureza_juridica_osc"]])
tb_dados_gerais[["dt_fundacao_osc"]] <- as_date(tb_dados_gerais[["dt_fundacao_osc"]])

# Esta variável tem um interpretação diferente no banco de dados antigo e novo (investigar!)
# tb_dados_gerais[["dt_ano_cadastro_cnpj"]] <- NA_Date_
tb_dados_gerais[["dt_ano_cadastro_cnpj"]] <- as_date(tb_dados_gerais[["dt_ano_cadastro_cnpj"]])

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = conexao_mosc, 
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
tb_contato <- readRDS(glue("{diretorio_att}output_files/tb_contato.RDS"))

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = conexao_mosc, 
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
tb_localizacao <- readRDS(glue("{diretorio_att}output_files/tb_localizacao.RDS"))

# Arrumar a classe das variáveis
tb_localizacao[["dt_geocodificacao"]] <- as_date(tb_localizacao[["dt_geocodificacao"]])
tb_localizacao[["cd_fonte_geocodificacao"]] <- as.integer(tb_localizacao[["cd_fonte_geocodificacao"]])
tb_localizacao[["tx_bairro_encontrado"]] <- as.character(tb_localizacao[["tx_bairro_encontrado"]])
tb_localizacao[["ft_bairro_encontrado"]] <- as.character(tb_localizacao[["ft_bairro_encontrado"]])

# Executa atualização
Atualizacao <- AtualizaDados(Conexao = conexao_mosc,
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
tb_area_atuacao <- readRDS(glue("{diretorio_att}output_files/tb_area_atuacao.RDS"))


# Corrige tipos de dado
tb_area_atuacao[["id_osc"]] <- as.integer(tb_area_atuacao[["id_osc"]])


# Executa atualização
Atualizacao <- AtualizaDados(Conexao = conexao_mosc,
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
Atualizacao <- AtualizaDados(Conexao = conexao_mosc,
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
  # i <- 16
  message(refresh_views[i])
  dbExecute(conexao_mosc, refresh_views[i])
}
rm(i, refresh_views)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finalização da rotina ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Atualiza controle de processos (tb_processos_atualizacao)  
if(!definicoes$att_teste) atualiza_processos_att(
  TipoAtt = "fim", 
  id_att = id_presente_att, 
  id_processo = 4)

# Desconecta da base
dbDisconnect(conexao_mosc)

rm(conexao_mosc)
rm(AtualizaDados)

# Fim ####
