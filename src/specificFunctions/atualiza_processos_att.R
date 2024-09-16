# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: função para atualizar as tabelas de atualização MOSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:


## Funções auxiliares:


## Outputs:


# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Debug ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# TipoAtt <- "fim"
# id_att <- 1
# path_file_backup = NULL
# path_file_backup <- "backup/2024_01/input_files/tb_JoinOSC.RDS"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Função ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
atualiza_processos_att <- function(
    TipoAtt = c("inicio", "fim"), # Determina se o processo está se iniciando ou finalizando
    id_att, # id da atualização
    id_processo, # id do processo
    processo_nome = NULL, # nome do processo
    path_file_backup = NULL) {
  
  # Checagens:
  assert_that(exists("conexao_mosc"))
  assert_that(dbIsValid(conexao_mosc))
  assert_that(length(TipoAtt) == 1)
  assert_that(any(c("inicio", "fim") %in% TipoAtt))
  assert_that(exists("tb_processos_atualizacao")) 
  # colocar aqui checagem dos objetos "dbplyr":
  assert_that(class(tb_processos_atualizacao) %in% "bla") # TO DO
  
  if(!is.null(path_file_backup)) assert_that(file.exists(path_file_backup)) 
  if(TipoAtt == "inicio") assert_that(!is.null(processo_nome)) 

  # Registra início do processo no controle de atualização:
  if(TipoAtt == "inicio") {
    
    # Evita repetição de linha:
    processo_nao_inserido <- tb_processos_atualizacao %>% 
      dplyr::filter(att_id == id_att, 
                    processo_id == id_processo) %>% 
      collect() %>% nrow() %>% 
      magrittr::equals(0)
    
    if(processo_nao_inserido) {
      
      rows_append(tb_processos_atualizacao, 
                  copy_inline(
                    conexao_mosc, 
                    tibble(att_id = id_att,
                           processo_id = id_processo,
                           tx_processo_nome = processo_nome,
                           bo_processo_att_completo = FALSE,
                           dt_processo_att_inicio = lubridate::now(),
                           dt_processo_att_fim = as_datetime(NA),   
                           nr_processo_att_controle = id_processo * 10,
                           
                           .rows = 1)), 
                  in_place = TRUE)
      
    } else {
      
      message("O processo já foi inserido")
      return(FALSE)
    }
    rm(processo_nao_inserido)
  }
  
  # Atualiza controle de atualização:
  if(TipoAtt == "fim") {
    
    # Atualiza realização de processos:
    rows_update(tb_processos_atualizacao, 
                copy_inline(
                  conexao_mosc, 
                  tibble(att_id = id_att,
                         processo_id = id_processo,
                         bo_processo_att_completo = TRUE,
                         dt_processo_att_fim = lubridate::now(),   
                         nr_processo_att_controle = id_processo * 10 + 1,
                         
                         .rows = 1)), 
                by = c("att_id", "processo_id"), 
                unmatched = "ignore",
                in_place = TRUE)
    
    # Registra arquivo intermediário criado:
    if(is.null(path_file_backup)) {
      
      assert_that(file.exists(path_file_backup))
      
      # id do backup (se a tabela está zerada, coloca 1)
      new_file_id <- ifelse(length(pull(tb_backups_files, file_id)) == 0, 1, 
                            max(pull(tb_backups_files, file_id), na.rm = TRUE) + 1)
      
      # Separa nome e diretório do arquivo
      file_name <- path_file_backup %>% 
        str_split("/") %>% 
        magrittr::extract2(1) %>% 
        magrittr::extract2(str_count(path_file_backup, "/") + 1)
      
      file_folder <- str_remove(path_file_backup, file_name)
      
      
      rows_append(tb_backups_files, 
                  copy_inline(conexao_mosc, 
                              tibble(
                                file_id = new_file_id,
                                att_id = id_att,
                                processo_id = id_processo,
                                tx_file_folder = file_name,
                                tx_file_name = file_folder,
                                nr_file_size_mb = file.size(path_file_backup)/1024000,
                                
                                .rows = 1)), 
                  in_place = TRUE)
      
      rm(new_file_id)
    }
  }
  return(TRUE)
}

# Fim