# Cria um controle para evitar inconsistência no campo "id_area_atuacao", 
# da tabela "tb_area_atuacao"

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-03-13


library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)
library(assertthat)

# Cria idAreaAtuacaoControl.RDS ####

# Carrega tabela com os dados tb_area_atuacao

tb_area_atuacao_OLD <- readRDS("data/temp/2024-07-08 extracao MOSC/tb_area_atuacao_teste.RDS")
# tb_area_atuacao_OLD <- dbGetQuery(connec, paste0("SELECT * FROM tb_area_atuacao_teste",
#                                                  # " LIMIT 1000",
#                                                  ";"))

names(tb_area_atuacao_OLD)

# Cria controle de chaves único para tb_area_atuacao
idAreaAtuacaoControl <- tb_area_atuacao_OLD %>% 
  # Evita repetir "id_osc" & "cd_area_atuacao" & "cd_subarea_atuacao"
  distinct(id_osc, cd_area_atuacao, cd_subarea_atuacao, 
           .keep_all = TRUE) %>% 
  # mantem somente variáveis importantes para as chaves
  select(id_area_atuacao, id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
  select(everything())

# Salva arquivo
saveRDS(idAreaAtuacaoControl, "tab_auxiliares/idAreaAtuacaoControl.RDS")  

# Limpa memória
rm(tb_area_atuacao_OLD, idAreaAtuacaoControl)
ls()

# Fim ####
