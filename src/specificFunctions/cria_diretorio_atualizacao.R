# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: 

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes 
# conexao_mosc (caso definicoes$att_teste == FALSE)
# diretorio_att
# processos_att_atual
# id_presente_att

## Funções auxiliares:
# "src/generalFunctions/postCon.R"
# "src/generalFunctions/agora.R"

## Outputs:
# Criação do Diretório de atualização ('diretorio_att')
# tb_processos_atualizacao (atualização)
# processos_att_atual (atualização)

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria diretório de atualização ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# "51": Processo 5 (Criação do diretório Backup) e 1 (completo)
if(!51 %in% processos_att_atual) {
  
  # Marca início do processo
  horario_processo_inicio <- now()
  
  if(!dir.exists(diretorio_att)) {
    
    message(agora(), ": Criando diretório de backup da atualização.")
    Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
    
    # Cria novos diretórios
    dir.create(diretorio_att)
    dir.create(glue("{diretorio_att}input_files/"))
    dir.create(glue("{diretorio_att}intermediate_files/"))
    dir.create(glue("{diretorio_att}output_files/"))
    
  }
  
  # Atualiza controle de processos:
  processos_att_atual <- unique(c(processos_att_atual, 51))
  
  if(!definicoes$att_teste) {
    
    rows_append(tb_processos_atualizacao, 
                copy_inline(conexao_mosc, 
                            tibble(att_id = id_presente_att,
                                   processo_id = 5,
                                   tx_processo_nome = "criação do diretório backup",
                                   bo_processo_att_completo = TRUE,
                                   dt_processo_att_inicio = horario_processo_inicio,
                                   dt_processo_att_fim = now(),   
                                   nr_processo_att_controle = 51,
                                   
                                   .rows = 1)), 
                in_place = TRUE)
    
  }
  
  rm(horario_processo_inicio)
  message("Diretório Backup dos arquivos criado") 
  
} else {

  # Verifica que o diretório de backup realmente foi criado:
  assert_that(all(dir.exists(c(glue("{diretorio_att}input_files/"), 
                               glue("{diretorio_att}intermediate_files/"), 
                               glue("{diretorio_att}output_files/")))),
              
              msg = glue("Há algum diretório de Backup que não existe. ", 
                         "Verifique porque o processo 51 consta no controle", 
                         " de atualizações!")) %>% 
    if(.) message("Diretórios Backup encontrados") 
  
}

Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Fim ####