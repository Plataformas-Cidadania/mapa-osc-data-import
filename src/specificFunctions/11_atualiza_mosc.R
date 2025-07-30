# Scrip para controlar as funções de atualização MOSC

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-03-01

library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)

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
# Carrega Dados ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Verifica se os arquivos existem:
assert_that(file.exists(glue("{diretorio_att}output_files/tb_osc.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_dados_gerais.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_area_atuacao.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_contato.RDS")))
assert_that(file.exists(glue("{diretorio_att}output_files/tb_localizacao.RDS")))



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega função de atualização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função de atualização de dados:
source("src/specificFunctions/BDConnection.R")

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
if(!exists('tb_osc')) {
  tb_osc <- readRDS(glue("{diretorio_att}output_files/tb_osc.RDS"))
}



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

# Carrega dados:
if(!exists('tb_osc')) {
  tb_dados_gerais <- readRDS(glue("{diretorio_att}output_files/tb_dados_gerais.RDS"))
}

# map_chr(tb_dados_gerais, class)

# Corrige tipos de dado
tb_dados_gerais[["dt_fechamento_osc"]] <- as_date(tb_dados_gerais[["dt_fechamento_osc"]])
tb_dados_gerais[["cd_natureza_juridica_osc"]] <- as.numeric(tb_dados_gerais[["cd_natureza_juridica_osc"]])
tb_dados_gerais[["dt_fundacao_osc"]] <- as_date(tb_dados_gerais[["dt_fundacao_osc"]])
tb_dados_gerais[["ft_fechamento_osc"]] <- as.character(tb_dados_gerais[["ft_fechamento_osc"]])

# TO DO: unificar esse campo ####
if(definicoes$Banco_Atualização == "Produção") {
  tb_dados_gerais[["cd_matriz_filial"]] <- as.integer(tb_dados_gerais[["cd_matriz_filial"]])
}
if(definicoes$Banco_Atualização == "Homologação") {
  tb_dados_gerais[["cd_matriz_filial"]] <- as.character(tb_dados_gerais[["cd_matriz_filial"]])
}

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
# Atualiza tb_dados_gerais (data de fechamento): ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# TO DO: colocar no mesmo padrão das outras bases o carregamento de dados

# Carrega dados RDS:
# DtFechamentoOSC <- readRDS(glue("{diretorio_att}intermediate_files/DtFechamentoOSC.RDS"))
DtFechamentoOSC <- readRDS(glue("{diretorio_att}intermediate_files/DtFechamentoOSC.RDS"))

# Carrega id_osc
old_data <- try(
  dbGetQuery(
    conexao_mosc, 
    paste0("SELECT id_osc, cd_identificador_osc FROM tb_osc",
           # " LIMIT 500", 
           ";")
  )
)

# View(old_data)

DtFechamentoOSC <- DtFechamentoOSC %>% 
  mutate(ft_fechamento_osc = paste0("CNPJ/SRF/MF/", codigo_presente_att), 
         dt_fechamento_osc = ymd(dt_fechamento_osc), 
         cd_identificador_osc = as.numeric(cd_identificador_osc), 
         dt_ultima_att = ymd(dt_ultima_att)
         ) %>% 
  left_join(old_data, by = "cd_identificador_osc")

rm(old_data)

# names(DtFechamentoOSC)
# sum(is.na(DtFechamentoOSC$id_osc))
# sum(is.na(DtFechamentoOSC$id_osc)) / nrow(DtFechamentoOSC)

# Faz upload da tabela temporária com as linhas de atualização
if(dbExistsTable(conexao_mosc, "update_temp")) dbRemoveTable(conexao_mosc, "update_temp")
dbWriteTable(conexao_mosc, "update_temp", DtFechamentoOSC)


# Insere atualizações de dt_fechamento_osc
query_JoinUpdate <- glue("UPDATE tb_dados_gerais \n",
                         " SET dt_fechamento_osc = update_temp.dt_fechamento_osc \n",
                         " FROM update_temp \n",
                         " WHERE tb_dados_gerais.id_osc",
                         " = update_temp.id_osc",
                         ";")
# query_JoinUpdate

# Executa a inserção das colunas
dbExecute(conexao_mosc, query_JoinUpdate)


# Insere atualizações de nr_ano_fechamento_osc
query_JoinUpdate <- glue("UPDATE tb_dados_gerais \n",
                         " SET nr_ano_fechamento_osc = update_temp.nr_ano_fechamento_osc \n",
                         " FROM update_temp \n",
                         " WHERE tb_dados_gerais.id_osc",
                         " = update_temp.id_osc",
                         ";")
# query_JoinUpdate

# Executa a inserção das colunas
dbExecute(conexao_mosc, query_JoinUpdate)


# Insere atualizações de ft_fechamento_osc
query_JoinUpdate <- glue("UPDATE tb_dados_gerais \n",
                         " SET ft_fechamento_osc = update_temp.ft_fechamento_osc \n",
                         " FROM update_temp \n",
                         " WHERE tb_dados_gerais.id_osc",
                         " = update_temp.id_osc",
                         ";")
# query_JoinUpdate

# Executa a inserção das colunas
dbExecute(conexao_mosc, query_JoinUpdate)


# Insere atualizações de dt_fechamento_osc
query_JoinUpdate <- glue("UPDATE tb_dados_gerais \n",
                         " SET dt_fechamento_osc = update_temp.dt_fechamento_osc \n",
                         " FROM update_temp \n",
                         " WHERE tb_dados_gerais.id_osc",
                         " = update_temp.id_osc",
                         ";")
# query_JoinUpdate

# Executa a inserção das colunas
dbExecute(conexao_mosc, query_JoinUpdate)

rm(query_JoinUpdate)

# Apaga a tabela temporária
if(dbExistsTable(conexao_mosc, "update_temp")) dbRemoveTable(conexao_mosc, "update_temp")

rm(DtFechamentoOSC)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza tb_contato: ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Inserindo dados da tabela 'tb_contato'")

# Carrega dados:
if(!exists('tb_contato')) {
  tb_contato <- readRDS(glue("{diretorio_att}output_files/tb_contato.RDS"))
}

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

# Carrega dados:
if(!exists('tb_localizacao')) {
  tb_localizacao <- readRDS(glue("{diretorio_att}output_files/tb_localizacao.RDS"))
}

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

# Carrega dados:
if(!exists('tb_area_atuacao')) {
  tb_area_atuacao <- readRDS(glue("{diretorio_att}output_files/tb_area_atuacao.RDS"))
}

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


if( definicoes$atualiza_RAIS ) {
  
  message("Inserindo dados da tabela 'tb_relacoes_trabalho'")
  
  # Carrega dados:
  if(!exists('tb_relacoes_trabalho')) {
    tb_relacoes_trabalho <- readRDS(glue("{diretorio_att}output_files/tb_relacoes_trabalho.RDS"))
  }
  
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
  
}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualiza dos views ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

assert_that(file.exists("tab_auxiliares/31_refresh_views_mat.sql"))

refresh_views <- read_lines("tab_auxiliares/31_refresh_views_mat.sql")

for (i in seq_along(refresh_views)) {
  # i <- 16
  message(refresh_views[i])
  try( dbExecute(conexao_mosc, refresh_views[i]))
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

processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 40], 41))

rm(AtualizaDados)

# Fim ####
