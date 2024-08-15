# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: baixa dados brutos da Receita Federal do Brasil, já filtrando
# as organizações sem fins lucrativos.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2023-10-19


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes 
# diretorio_att
# processos_att_atual
# id_presente_att
# Caso definicoes$att_teste == FALSE:
## conexao_mosc 
## tb_backups_files
## tb_processos_atualizacao


## Funções auxiliares:
# "src/generalFunctions/postCon.R"
# "src/generalFunctions/agora.R"

## Outputs:
# tb_JoinOSC (objeto da memória e, caso definicoes$att_teste == FALSE, arquivo salvo)

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres) 


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega Dados da RFB ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa os dados da Receita, se não tiver sido feito ainda
# "11": Processo 1 (baixar bases de dados brutas RFB) e 1 (completo)
if(!(11 %in% processos_att_atual)) {

  ## Checagens preliminares ####
  assert_that("credenciais_rfb" %in% names(definicoes), 
              msg = "Não foi encontrado 'credenciais_rfb' nas definições!")
  
  assert_that("schema_receita" %in% names(definicoes), 
              msg = "Não foi encontrado 'schema_receita' nas definições!")
  
  message(agora(), "  Carrega Dados da RFB")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  # Início do processo ####
  horario_processo_inicio <- now()
  processos_att_atual <- c(processos_att_atual[processos_att_atual != 11], 10)
  
  # Registra início do processo no controle de atualização:
  if(!definicoes$att_teste) {
    
    # Evita repetição de linha:
    processo_nao_inserido <- tb_processos_atualizacao %>% 
      dplyr::filter(att_id == id_presente_att, processo_id == 1) %>% 
      collect() %>% nrow() %>% 
      magrittr::equals(0)
    
    if(processo_nao_inserido) {
      
      rows_append(tb_processos_atualizacao, 
                  copy_inline(conexao_mosc, 
                              tibble(att_id = id_presente_att,
                                     processo_id = 1,
                                     tx_processo_nome = "criação do diretório backup",
                                     bo_processo_att_completo = FALSE,
                                     dt_processo_att_inicio = horario_processo_inicio,
                                     dt_processo_att_fim = as_datetime(NA),   
                                     nr_processo_att_controle = 10,
                                     
                                     .rows = 1)), 
                  in_place = TRUE)
      
    }
    rm(processo_nao_inserido)
    
  }
  
  # Conecta à nase de dados da Receita Federal:
  source("src/generalFunctions/postCon.R") 
  conexao_rfb <- postCon(definicoes$credenciais_rfb, 
                         Con_options = 
                           glue(
                             "-c search_path={definicoes$schema_receita}"))
  if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")
  rm(postCon)

  # Query para buscar as informações nas tabelas "tb_rfb_empresas" e 
  # "tb_rfb_estabelecimentos". Já faz o Join entre as tabelas e também 
  # filtra pela natureza jurídica das organizações sem fins lucrativos
  # ('3069', '3220', '3301', '3999')
  
  # Filtra naturezas jurídicas:
  # grupo 3: Entidades sem fins lucrativos
  ## 306-9	Fundação Privada
  ## 322-0	Organização Religiosa
  ## 330-1	Organização Social (OS)
  ## 399-9	Associação Privada
  # Fonte: https://concla.ibge.gov.br/estrutura/natjur-estrutura/natureza-juridica-2021

  # Por algum motivo, o glue não funciona bem com listas:
  tabela_empresas_rfb <- definicoes$tabela_empresas_rfb[[1]]
  tabela_estabelecimentos_rfb <- definicoes$tabela_estabelecimentos_rfb[[1]]
  campo_cnpj <- definicoes$campo_rfb_cnpj[[1]]
  campo_natureza_juridica <- definicoes$campo_rfb_natureza_juridica[[1]]
  
  # Query para resgatar ao mesmo tempo dados de empresas e estabelecimentos
  # filtrando por naturezas jurídicas não lucraticas, na Receita Federal:
  query_naolucrativo_rfb <- glue(
    "SELECT * FROM {tabela_empresas_rfb} 
      RIGHT JOIN {tabela_estabelecimentos_rfb} 
      ON {definicoes$tabela_estabelecimentos_rfb}.{campo_cnpj} \\
      = {definicoes$tabela_empresas_rfb}.{campo_cnpj} 
      WHERE {campo_natureza_juridica} IN ('",
    paste(definicoes$natjur_nao_lucarivo, collapse = "', '"),
    "')",
    # " LIMIT 1000", 
    ";")
  
  rm(tabela_empresas_rfb, tabela_estabelecimentos_rfb, campo_cnpj, 
     campo_natureza_juridica)
  
  # Faz a busca na receita federal ####
  message(agora(), "  Iniciando busca nos dados da Receita Federal")
  
  tb_JoinOSC <- dbGetQuery(conexao_rfb, query_naolucrativo_rfb)
  
  message(agora(), "  Dados da Receita Federal Baixados")
  
  rm(query_naolucrativo_rfb)
  
  # Caminho do arquivo Backup
  path_file_backup <- glue("{diretorio_att}intermediate_files/tb_JoinOSC.RDS")
  
  # Salva arquivo Backup ####
  if(definicoes$salva_backup) saveRDS(tb_JoinOSC, path_file_backup)
  
  # Atualiza controle de processos ####
  processos_att_atual <- c(processos_att_atual[processos_att_atual != 10], 11)
  
  # Atualiza controle de atualização:
  if(!definicoes$att_teste) {
    
    # Atualiza realização de processos:
    rows_update(tb_processos_atualizacao, 
                copy_inline(conexao_mosc, 
                            tibble(att_id = id_presente_att,
                                   processo_id = 1,
                                   tx_processo_nome = "baixar bases de dados brutas RFB",
                                   bo_processo_att_completo = TRUE,
                                   dt_processo_att_inicio = horario_processo_inicio,
                                   dt_processo_att_fim = now(),   
                                   nr_processo_att_controle = 11,
                                   
                                   .rows = 1)), 
                by = c("att_id", "processo_id"), 
                unmatched = "ignore",
                in_place = TRUE)
    
    # Registra arquivo intermediário criado:
    if(definicoes$salva_backup) {
      
      new_file_id <- ifelse(length(pull(tb_backups_files, file_id)) == 0, 1, 
                            max(pull(tb_backups_files, file_id), na.rm = TRUE) + 1)
      
      rows_append(tb_backups_files, 
                  copy_inline(conexao_mosc, 
                              tibble(
                                file_id = new_file_id,
                                att_id = id_presente_att,
                                processo_id = 1,
                                tx_file_folder = glue("{diretorio_att}input_files/"),
                                tx_file_name = "tb_JoinOSC.RDS",
                                nr_file_size_mb = file.size(path_file_backup)/1024000,
                                
                                .rows = 1)), 
                  in_place = TRUE)
      
      rm(new_file_id)
      
    }
    
  }
  
  rm(path_file_backup, horario_processo_inicio)
  
  dbDisconnect(conexao_rfb)
  rm(conexao_rfb)
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("tb_JoinOSC") || 
                file.exists(
                  glue("{diretorio_att}intermediate_files/tb_JoinOSC.RDS")),
              
              msg = glue("Não foi encontrado os dados da Receita Federal na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 11 consta como concluído!")) %>% 
    if(.)  message("Dados da RFB já carregados anteriormente")
  
  }


# Fim ####
