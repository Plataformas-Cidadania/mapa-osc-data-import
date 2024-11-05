# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Insere dados da RAIS no Mapa das Organizações Sociais

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere Dados da RAIS ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito anteriormente
# "81": Processo 8 (Update Banco de Dados MOSC) e 1 (completo)
if(!(81 %in% processos_att_atual)) {
  
  message("Insere os dados da RAIS")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 8, 
    processo_nome = "Insere Dados RAIS")
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  # Estou aqui !!!! #####
  
  # Conecta à base de dados da RAIS:
  source("src/generalFunctions/postCon.R") 
  conexao_rfb <- postCon(definicoes$credenciais_rfb, 
                         Con_options = 
                           glue(
                             "-c search_path={definicoes$schema_receita}"))
  if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")
  rm(postCon)
  
  ListRawFiles <- list.files(paste0(DirName, "input_files/RAIS/"), 
                             pattern = "RDS$")
  
  # Base de dados para reunir os dados
  GatherData <- tibble()
  
  for (i in seq_along(ListRawFiles)) {
    # i <- 1
    
    message("Inserindo dados de ", 
            str_sub(ListRawFiles[i], 6, 7), ", Ano: ", 
            str_sub(ListRawFiles[i], 1, 4), ", natureza jurídica: ",
            str_sub(ListRawFiles[i], 9, 12))
    
    # Lê dados brutos
    rawdata <- readRDS(paste0(DirName, "input_files/RAIS/", ListRawFiles[i]))
    # names(rawdata)
    names(idControl)
    
    # Processa os dados
    new_rows <- rawdata %>% 
      rename(cd_identificador_osc = id_estab) %>% 
      group_by(ano, cd_identificador_osc) %>% 
      summarise(nr_trabalhadores_vinculo = n(),
                ft_trabalhadores_vinculo = NA,
                nr_trabalhadores_deficiencia = sum(ind_defic, na.rm = TRUE),
                ft_trabalhadores_deficiencia = NA,
                nr_trabalhadores_voluntarios = NA_integer_,
                ft_trabalhadores_voluntarios = NA_character_) %>% 
      mutate(ft_trabalhadores_vinculo = paste("RAIS/MTE", ano), 
             ft_trabalhadores_deficiencia = paste("RAIS/MTE", ano)) %>% 
      left_join(idControl, by = "cd_identificador_osc") %>% 
      select(-cd_identificador_osc) %>% 
      select(ano, id_osc, everything())
    
    # Insire os dados extraidos na base
    GatherData <- bind_rows(GatherData, new_rows)
    
    rm(rawdata, new_rows)
  }
  rm(i)
  
  # Remove caso de id_osc nulo
  GatherData <- GatherData[!is.na(GatherData$id_osc), ]
  
  # table(GatherData$ano)
  
  # Versão dos dados 1 (igual no MOSC, sem o ano)
  tb_relacoes_trabalho <- GatherData %>% 
    group_by(id_osc) %>% 
    dplyr::filter(ano == max(ano)) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(-ano)
  
  # Versão dos dados 1 (adicionando o ano)
  tb_relacoes_trabalho2 <- GatherData %>% 
    group_by(id_osc, ano) %>% 
    dplyr::filter(ano == max(ano)) %>% 
    slice(1) %>% 
    ungroup()
  
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_relacoes_trabalho.RDS")
  saveRDS(tb_relacoes_trabalho, PathFile)
  
  PathFile <- paste0(DirName, "output_files/tb_relacoes_trabalho2.RDS")
  saveRDS(tb_relacoes_trabalho2, PathFile)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_8"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_relacoes_trabalho.RDS", 
            FileSizeMB = file.size(paste0(DirName, "output_files/tb_relacoes_trabalho.RDS"))/1024000 ) %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_8"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_relacoes_trabalho2.RDS", 
            FileSizeMB = file.size(paste0(DirName, "output_files/tb_relacoes_trabalho2.RDS"))/1024000 )
  
  # Atualiza realização de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_8"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 6,
            Processo_Nome = "Insere Dados da RAIS",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "81")
  
  rm(tb_relacoes_trabalho, tb_relacoes_trabalho2, GatherData)  
  rm(DataProcessoInicio, ListRawFiles)
  # ls()
}


# Fim ####