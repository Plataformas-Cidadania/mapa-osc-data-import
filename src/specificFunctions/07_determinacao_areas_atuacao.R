# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: fazer uma estimativa das áreas de atuação das OSC
# com base na CNAE

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
if( !(31 %in% processos_att_atual) ) {
  
  message("Determinação das áreas de atuação OSC")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 31], 30))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 3, 
    processo_nome = "Determinação das áreas de atuação OSC")
  
  # Se os dados não estiverem carregados, carrega eles. ####
  if(!(exists("Tb_OSC_Full") && "data.frame" %in% class(Tb_OSC_Full))) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    PathFile <- glue("{diretorio_att}intermediate_files/Tb_OSC_Full.RDS")
    
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
  
  message(agora(), "   Início da rotina de determinação das áreas")
  
  # Função para determinar as áreas de atuação
  source("src/specificFunctions/AreaAtuacaoOSC.R")
  
  # Transforma Tb_OSC_Full em DB_OSC
  
  CamposAtualizacao <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita)
  
  campo_rfb_cnae_principal <- CamposAtualizacao %>% 
    dplyr::filter(campos == "campo_rfb_cnae_principal") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  DB_OSC <- Tb_OSC_Full %>%
    mutate(cnae = .data[[campo_rfb_cnae_principal]], 
           micro_area_atuacao = NA)
  
  rm(campo_rfb_cnae_principal)
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
  
  message(agora(), "   Final da rotina de terminação das áreas")
  
  # Salva Backup - CNAE Principal ####
  path_file_backup <- glue("{diretorio_att}intermediate_files/DB_OSC.RDS")
  
  if(definicoes$salva_backup) saveRDS(DB_OSC, path_file_backup)
  
  # Atualiza controle de processos - CNAE Principal ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 30], 31))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 3, 
    path_file_backup = ifelse(definicoes$salva_backup, path_file_backup, NULL))
  
  message(agora(), "   Rotida de determinação das áreas usando a CNAE secundária")

  # Determina área de atuação secundária  - Multi áreas ####
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 8, 
    processo_nome = "Determinação das áreas de atuação com a CNAE secundária (multiáreas)")
  
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
  
  message(agora(), "   Fim da determinação das áreas usando a CNAE secundária")
  
  # names(MultiAreas)
  # View(MultiAreas)  
  # table(MultiAreas$OrdemArea)
  # table(MultiAreas$micro_area_atuacao)
  
  if(definicoes$salva_backup) saveRDS(MultiAreas, 
                                      glue("{diretorio_att}intermediate_files/MultiAreasAtuacao.RDS"))
  
  # Salva arquivo Backup - Multi áreas ####
  
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 30], 31))
  
  # Atualiza controle de processos (tb_processos_atualizacao) 
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 8, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              glue("{diretorio_att}intermediate_files/MultiAreasAtuacao.RDS"),
                              NULL))
  
  rm(AreaAtuacaoOSC, DB_SubAreaRegras, DB_AreaSubaria)
  rm(horario_processo_inicio, path_file_backup)
  rm(MultiAreas)
  gc()
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