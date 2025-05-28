# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: procedimentos finais da rotina de atualização MOSC:
# - Cria uma cópia de todos os arquivos de código e tabelas auxiliares
# utilizadas na atualização.
# - Insere os dados de finalização na tabela de atualização.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2025-05-28

# bibliotecas necessárias:
library(dplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cópia dos arquivos de código da atualização  ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Faz a cópia da rotina utilizada

# Salva definições:
save(definicoes, 
     file = paste0(diretorio_att, 'replication_files/', "definicoes.RData")
     )

# Diretórios onde estão os arquivos fundamentais da atualização
codigos_dir <- c(
  "src", 
  "src/generalFunctions",
  "src/specificFunctions", 
  "tab_auxiliares"
)

assert_that(
  all(
    dir.exists(codigos_dir)
  )
)

# Copia os arquivos utilizados na atualização para o backup
for (i in seq_along(codigos_dir)) {
  # i <- 1
  dir_destiny <- paste0(diretorio_att, 'replication_files/', codigos_dir[i], "/")
  
  if( !dir.exists(dir_destiny) ) dir.create(dir_destiny)
  
  files_dir_i <- list.files(codigos_dir[i], full.names = TRUE)
  
  for (j in seq_along(files_dir_i) ) {
    # j <- 1
    if( is.dir( files_dir_i[j] ) ) next
    
    message("Copiando arquivo ", files_dir_i[j])
    
    file.copy(
      from = files_dir_i[j], 
      to = paste0(diretorio_att, 'replication_files/', files_dir_i[j])
    )
  }
  rm(j, dir_destiny, files_dir_i)
}
rm(i, codigos_dir)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere a finalização da rotina no controle de atualização  ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Insere o comentário da Atualização
query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
                     " SET tx_att_comentarios = '{definicoes$tx_att_comentarios}' \n",
                     " WHERE att_id = {id_presente_att}",
                     ";")

# query_AltRow

dbExecute(conexao_mosc, query_AltRow)

# Coloca a data de finalização da atualização

horario_fim <- agora()

query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
                     " SET dt_att_fim = '{horario_fim}'::timestamp \n",
                     " WHERE att_id = {id_presente_att}",
                     ";")

# query_AltRow

dbExecute(conexao_mosc, query_AltRow)

rm(horario_fim)

# Coloca status da atualização como finalizada
query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
                     " SET tx_att_situacao = 'finalizada' \n",
                     " WHERE att_id = {id_presente_att}",
                     ";")

# query_AltRow

dbExecute(conexao_mosc, query_AltRow)

rm(query_AltRow)


# Fim ####