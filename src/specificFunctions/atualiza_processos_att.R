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
# TipoAtt <- "arquivo backup"
# id_att <- id_presente_att
# id_processo <- 6
# processo_nome = "criação do diretório backup"
# path_file_backup = NULL
# path_file_backup <- path_file

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Função ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
atualiza_processos_att <- function(
    # Determina se o processo está se iniciando ou finalizando:
    TipoAtt = c("inicio", "fim", "arquivo backup", "remove linha backups_files"), 
    id_att, # id da atualização
    id_processo, # id do processo
    processo_nome = NULL, # nome do processo
    path_file_backup = NULL # Arquivo de backup de atualização:
    ) {
  
  # Tipos implementados de atualização:
  tipos_atualizacao <- c("inicio", "fim", "arquivo backup", 
                         "remove linha backups_files")
  
  # Checagens:
  assert_that(exists("conexao_mosc"))
  assert_that(dbIsValid(conexao_mosc))
  assert_that(length(TipoAtt) == 1)
  assert_that(TipoAtt  %in% tipos_atualizacao)
  assert_that(exists("tb_processos_atualizacao")) 
  assert_that(any(class(tb_processos_atualizacao) %in% "tbl_PqConnection"))
  
  if(TipoAtt == "arquivo backup") assert_that(!is.null(path_file_backup))
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
    if(!is.null(path_file_backup)) {
      
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
  
  if(TipoAtt == "arquivo backup") {
    
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
    
    rm(new_file_id, file_name, file_folder)
    
  }
  
  if(TipoAtt == "remove linha backups_files") {
    
    # Separa nome e diretório do arquivo
    file_name <- path_file_backup %>% 
      str_split("/") %>% 
      magrittr::extract2(1) %>% 
      magrittr::extract2(str_count(path_file_backup, "/") + 1)
    
    file_folder <- str_remove(path_file_backup, file_name)
    
    rows_delete(tb_backups_files, 
                copy_inline(conexao_mosc, 
                            tibble(
                              att_id = id_att,
                              processo_id = id_processo,
                              tx_file_folder = file_name,
                              tx_file_name = file_folder,
                              
                              .rows = 1)),
                by = c("att_id", "processo_id", "tx_file_folder", 
                       "tx_file_name"),
                unmatched = "ignore",
                in_place = TRUE) 
  }
  
  return(TRUE)
}

# Fim