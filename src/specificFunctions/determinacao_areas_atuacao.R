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
# Tb_OSC_Full (objeto na memódia ou arquivo em "{diretorio_att}intermediate_files")
# diretorio_att
# processos_att_atual
# Caso definicoes$att_teste == FALSE:
## conexao_mosc 
## tb_backups_files
## tb_processos_atualizacao

## Funções auxiliares:
# "src/generalFunctions/agora.R"
# "src/specifcFuntions/AreaAtuacaoOSC.R"

## Outputs:
# Tb_OSC_Full (objeto da memória e, caso definicoes$att_teste == FALSE, arquivo salvo)


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
# Determinação das áreas de atuação OSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito
# "31": Processo 2 (Determinação das áreas de atuação OSC) e 1 (completo)
if(!(31 %in% processos_att_atual)) {
  
  message("Determinação das áreas de atuação OSC")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  horario_processo_inicio <- now()
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 31], 30))

  # Registra início do processo no controle de atualização:
  if(!definicoes$att_teste) {
    
    # Evita repetição de linha:
    processo_nao_inserido <- tb_processos_atualizacao %>% 
      dplyr::filter(att_id == id_presente_att, processo_id == 3) %>% 
      collect() %>% nrow() %>% 
      magrittr::equals(0)
    
    if(processo_nao_inserido) {
      
      rows_append(tb_processos_atualizacao, 
                  copy_inline(conexao_mosc, 
                              tibble(att_id = id_presente_att,
                                     processo_id = 3,
                                     tx_processo_nome = "Determinação das áreas de atuação OSC",
                                     bo_processo_att_completo = FALSE,
                                     dt_processo_att_inicio = horario_processo_inicio,
                                     dt_processo_att_fim = as_datetime(NA),   
                                     nr_processo_att_controle = 30,
                                     
                                     .rows = 1)), 
                  in_place = TRUE) 
      
    }
    
    rm(processo_nao_inserido)
    
  }
  
  # Se os dados não estiverem carregados, carrega eles. ####
  if(!(exists("Tb_OSC_Full") && "data.frame" %in% class(Tb_OSC_Full))) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    PathFile <- paste0(DirName, "intermediate_files/Tb_OSC_Full.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(PathFile), 
                msg = paste0("Arquivo '", PathFile, "' não encontrado."))
    
    # Carrega ele
    Tb_OSC_Full <- readRDS(PathFile)
    # names(Tb_OSC_Full)
    # nrow(Tb_OSC_Full)
    rm(PathFile)
  }
  
  # Regras para determinar as subáreas de atuação
  DB_SubAreaRegras <- fread("tab_auxiliares/IndicadoresAreaAtuacaoOSC.csv",
                            encoding = "Latin-1")
  
  # Relação entre micro áreas e macro áreas
  DB_AreaSubaria <- fread("tab_auxiliares/Areas&Subareas.csv",
                          encoding = "Latin-1")
  
  # Função para determinar as áreas de atuação
  source("src/specificFunctions/AreaAtuacaoOSC.R")
  
  # Transforma Tb_OSC_Full em DB_OSC
  DB_OSC <- Tb_OSC_Full %>%
    mutate(cnae = cnae_fiscal_principal, 
           micro_area_atuacao = NA)
  
  rm(Tb_OSC_Full) # não vamos mais utilizar esses dados
  
  # Usa função "AreaAtuacaoOSC" para determinar qual a área de atuação
  # das OSCs
  DB_OSC$micro_area_atuacao <- AreaAtuacaoOSC(select(DB_OSC, 
                                                     cnpj_basico, 
                                                     razao_social, 
                                                     cnae, 
                                                     micro_area_atuacao), 
                                              DB_SubAreaRegras, 
                                              chuck_size = 10000, verbose = FALSE)
  
  DB_OSC <- DB_OSC %>% 
    # Se não foi indentificado pelo sistema, colocar "Outras"
    mutate(micro_area_atuacao = ifelse(is.na(micro_area_atuacao), 
                                       "Outras organizações da sociedade civil", 
                                       micro_area_atuacao)) %>% 
    # Insere Macro áreas de atuação
    left_join(DB_AreaSubaria, by = "micro_area_atuacao")
  
  # Salva Backup - CNAE Principal ####
  path_file_backup <- glue("{diretorio_att}intermediate_files/DB_OSC.RDS")
  
  if(definicoes$salva_backup) saveRDS(DB_OSC, path_file_backup)
  
  # Atualiza controle de processos - CNAE Principal ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 30], 31))
  
  if(!definicoes$att_teste) {
  
    # Atualiza realização de processos:
    rows_update(tb_processos_atualizacao, 
                copy_inline(conexao_mosc, 
                            tibble(att_id = id_presente_att,
                                   processo_id = 3,
                                   tx_processo_nome = "Determinação das áreas de atuação OSC",
                                   bo_processo_att_completo = TRUE,
                                   dt_processo_att_inicio = horario_processo_inicio,
                                   dt_processo_att_fim = now(),   
                                   nr_processo_att_controle = 31,
                                   
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
                                tx_file_folder = glue("{diretorio_att}intermediate_files/"),
                                tx_file_name = "DB_OSC.RDS",
                                nr_file_size_mb = file.size(path_file_backup)/1024000,
                                
                                .rows = 1)), 
                  in_place = TRUE)
      
      rm(new_file_id)
    }
    
  }

  # Determina área de atuação secundária  - Multi áreas ####
  MultiAreas <- DB_OSC %>% 
    mutate(cnae = cnae_fiscal_secundaria) %>% 
    select(cnpj, razao_social, cnae) %>% 
    separate(cnae, into = letters[1:26], 
             sep = ",", fill = "right") %>% 
    gather(temp, cnae, 3:28) %>%  
    dplyr::filter(!is.na(cnae)) %>% 
    select(-temp) %>% 
    mutate(cnae = trimws(cnae), 
           tipo_cnae = "s") %>% 
    group_by(cnpj) %>% 
    mutate(OrdemArea = row_number()) %>% 
    ungroup()
  
  # Existe um pequeno número de OSC que tem um número exagerado de CNAEs
  # secundárias que serão discartadas nesse metodologia.
  
  MultiAreas$micro_area_atuacao <- NA
  MultiAreas$micro_area_atuacao <- AreaAtuacaoOSC(MultiAreas, 
                                                  DB_SubAreaRegras, 
                                                  chuck_size = 10000, verbose = FALSE)
  
  # names(MultiAreas)
  # View(MultiAreas)  
  # table(MultiAreas$OrdemArea)
  # table(MultiAreas$micro_area_atuacao)
  
  # Salva arquivo Backup - Multi áreas ####
  
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 90], 91))
  
  if(definicoes$salva_backup) {
    
    path_file_backup <- glue("{diretorio_att}intermediate_files/MultiAreasAtuacao.RDS")
    
    # Salva dados das atuações secundárias
    saveRDS(MultiAreas, path_file_backup)
    
    if(!definicoes$att_teste) {
      
      # Evita repetição de linha:
      processo_nao_inserido <- tb_processos_atualizacao %>% 
        dplyr::filter(att_id == id_presente_att, processo_id == 9) %>% 
        collect() %>% nrow() %>% 
        magrittr::equals(0)
      
      if(processo_nao_inserido) {
        
        rows_append(tb_processos_atualizacao, 
                    copy_inline(conexao_mosc, 
                                tibble(att_id = id_presente_att,
                                       processo_id = 9,
                                       tx_processo_nome = "Determinação das áreas de atuação com a CNAE secundária (multiáreas)",
                                       bo_processo_att_completo = TRUE,
                                       dt_processo_att_inicio = horario_processo_inicio,
                                       dt_processo_att_fim = now(),   
                                       nr_processo_att_controle = 91,
                                       
                                       .rows = 1)), 
                    in_place = TRUE) 
        
      }
      
      new_file_id <- ifelse(length(pull(tb_backups_files, file_id)) == 0, 1, 
                            max(pull(tb_backups_files, file_id), na.rm = TRUE) + 1)
      
      rows_append(tb_backups_files, 
                  copy_inline(conexao_mosc, 
                              tibble(
                                file_id = new_file_id,
                                att_id = id_presente_att,
                                processo_id = 3,
                                tx_file_folder = glue("{diretorio_att}intermediate_files/"),
                                tx_file_name = "MultiAreasAtuacao.RDS",
                                nr_file_size_mb = file.size(path_file_backup)/1024000,
                                
                                .rows = 1)), 
                  in_place = TRUE)
      
      rm(new_file_id)
      
    }
    
  }
  
  rm(AreaAtuacaoOSC, DB_SubAreaRegras, DB_AreaSubaria)
  rm(horario_processo_inicio, path_file_backup)
  rm(MultiAreas)
  # ls()
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("DB_OSC") || 
                file.exists(
                  glue("{diretorio_att}intermediate_files/DB_OSC.RDS")),
              
              msg = glue("Não foi encontrado o objeto 'DB_OSC.RDS' na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 31 consta como concluído!")) %>% 
    if(.)  message("Determinação das áreas de atuação OSC já feita anteriormente")
  
  }


# Fim ####