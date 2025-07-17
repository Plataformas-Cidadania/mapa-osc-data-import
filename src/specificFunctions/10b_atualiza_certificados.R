# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Atualiza os dados dos certificados

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2025-07-10

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:

## Funções auxiliares:

## Outputs:
# (nenhum)

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
library(readxl) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere Dados da RAIS ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito anteriormente
# "101": Processo 10 (Atualização dos Certificados) e 1 (completo)
if(!(101 %in% processos_att_atual)) {
  
  message("Atualiza Certificados")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 101], 100))

  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 10, 
    processo_nome = "Atualiza Certificados")
  
  source("src/generalFunctions/Check_PK_Rules.R")
  source("src/generalFunctions/Empty2NA.R")
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  # Resgata os dados já existentes sobre os certificados
  tb_id_certificados <- tbl(conexao_mosc, "tb_certificado") %>% 
    select(id_certificado, id_osc, cd_certificado) %>% 
    collect()
  
  
  ListRawFiles <- list.files("input_files_next", "^tb_certificado")
  
  # Base de dados para reunir os dados
  tb_certificado <- tibble()
  
  for (i in seq_along(ListRawFiles)) {
    # i <- 1
    
    encodguess <- glue("input_files_next/{ListRawFiles[i]}") %>% 
      guess_encoding() %>% 
      magrittr::extract2(1) %>% 
      magrittr::extract2(1) %>%
      magrittr::equals(c("ISO-8859-2", "ISO-8859-1", "Latin-1")) %>% 
      any() %>% 
      ifelse("Latin-1", 'UTF-8')
    
    if(str_detect(ListRawFiles[i], "\\.csv$")) {
      RawFile <- fread(glue("input_files_next/{ListRawFiles[i]}"), 
                       encoding = encodguess)
    }
    
    if(str_detect(ListRawFiles[i], "\\.xlsx$|\\.xls$|\\.xlsm$")) {
      RawFile <- read_xlsx(glue("input_files_next/{ListRawFiles[i]}"), 
                           sheet = 1)
    }
    
    assert_that(
      all(
        c("cd_certificado", "dt_inicio_certificado", "dt_fim_certificado",
          "bo_oficial", "cd_municipio", "cd_uf", 
          "ft_certificado") %in% names(RawFile)), 
      
      msg = paste0("Falta os seguintes campos na tabela", ListRawFiles[i], ": ",
                   names(RawFile)[
                     !(
                       c("cd_certificado", "dt_inicio_certificado",
                         "dt_fim_certificado", "bo_oficial",
                         "cd_municipio", "cd_uf",
                         "ft_certificado") %in% names(RawFile)
                     )]))
      
    
    NewRows <- RawFile %>% 
      mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                            width = 14, 
                                            side = "left", 
                                            pad = "0")) %>% 
      # Adicona id_osc
      left_join(idControl, by = "cd_identificador_osc") %>% 
      
      # Somente inserir dados completos
      dplyr::filter(!is.na(id_osc), 
                    !is.na(dt_inicio_certificado), 
                    !dt_inicio_certificado == "") %>% 
      
      # inserir id_certificado
      left_join(tb_id_certificados, by = c("cd_certificado", "id_osc")) %>% 
      
      # Finaliza Variáveis
      mutate(
        ft_inicio_certificado = ft_certificado,
        ft_fim_certificado = ft_certificado,
        ft_municipio = ft_certificado,
        ft_uf = ft_certificado
      ) %>% 
    
      select(id_certificado,
             id_osc,
             cd_certificado,
             tx_certificado,
             dt_inicio_certificado,
             dt_fim_certificado,
             bo_oficial,
             cd_municipio,
             cd_uf,
             ft_certificado,
             ft_inicio_certificado,
             ft_fim_certificado,
             ft_municipio,
             ft_uf)
    
    tb_certificado <- bind_rows(tb_certificado, NewRows)
    
    # Transfere arquivo de "input_files_next" para "diretorio_input_att"
    file.rename(
      from = glue( "input_files_next/{ListRawFiles[i]}"), 
      to = glue("{diretorio_att}input_files/{ListRawFiles[i]}")
    )
    
    rm(encodguess, RawFile, NewRows)
  }
  rm(i, ListRawFiles)
  
  
  # Cria novos números de id_certificado
  if(sum(is.na(tb_certificado$id_certificado)) > 0) {
    # id máximo antigo
    Max_OldID <- max(tb_id_certificados$id_certificado, na.rm = TRUE)
    
    # Novos IDs
    NewID <- seq(from = Max_OldID + 1, 
                 to = Max_OldID + sum(is.na(tb_certificado$id_certificado)), 
                 by = 1)
    
    # Adiciona novos IDs
    tb_certificado$id_certificado[is.na(tb_certificado$id_certificado)] <- NewID
    
    assert_that(sum(is.na(tb_certificado$id_certificado)) == 0)

    rm(Max_OldID, NewID)
  }
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_certificado$id_certificado)
  
  # Remove strings vazias ("")
  tb_certificado <- Empty2NA(tb_certificado)
  
  if(definicoes$salva_backup) {
    # Salva Backup
    PathFile <- paste0(DirName, "output_files/tb_certificado.RDS")
    saveRDS(tb_certificado, PathFile)
    
    if(!definicoes$att_teste) {
      # Registra novo arquivo salvo
      BackupsFiles <- BackupsFiles %>% 
        add_row(
          ControleAt_Id = paste0(Att_Atual$At_id[1], "_10"), 
          FileFolder = paste0(DirName, "output_files/"), 
          FileName = "tb_certificado.RDS", 
          FileSizeMB = file.size(paste0(DirName, "output_files/tb_certificado.RDS"))/1024000 
        )
    }
  }
   
  # Atualiza realização de processos:
    
  # Atualiza controle de processos ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 100], 101))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 10)
  
  rm(tb_certificado, tb_id_certificados, Empty2NA, Check_PK_Rules, idControl)
  # ls()
}


# Fim ####