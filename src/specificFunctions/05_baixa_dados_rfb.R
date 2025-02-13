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
  processos_att_atual <- c(processos_att_atual[processos_att_atual != 11], 10)
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 1, 
    processo_nome = "baixa dados da Receita Federal")
  
  # Conecta à base de dados da Receita Federal:
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

  # Carrega os campos necessários para os testes abaixo.
  CamposAtualizacao <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita)
  
  tabela_empresas_rfb <- CamposAtualizacao %>% 
    dplyr::filter(campos == "tabela_empresas_rfb") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
    
  tabela_estabelecimentos_rfb <- CamposAtualizacao %>% 
    dplyr::filter(campos == "tabela_estabelecimentos_rfb") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  campo_cnpj <- CamposAtualizacao %>% 
    dplyr::filter(campos == "campo_rfb_cnpj") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  campo_natureza_juridica <- CamposAtualizacao %>% 
    dplyr::filter(campos == "campo_rfb_natureza_juridica") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  natjur_nao_lucrativo <- CamposAtualizacao %>% 
    dplyr::filter(campos == "natjur_nao_lucarivo") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character() %>% 
    str_split(fixed("|")) %>% magrittr::extract2(1)
  
  rm(CamposAtualizacao)
  
  # Query para resgatar ao mesmo tempo dados de empresas e estabelecimentos
  # filtrando por naturezas jurídicas não lucraticas, na Receita Federal:
  query_naolucrativo_rfb <- glue(
    "SELECT * FROM {tabela_empresas_rfb} 
      RIGHT JOIN {tabela_estabelecimentos_rfb} 
      ON {tabela_estabelecimentos_rfb}.{campo_cnpj} \\
      = {tabela_empresas_rfb}.{campo_cnpj} 
      WHERE {campo_natureza_juridica} IN ('",
    paste(natjur_nao_lucrativo, collapse = "', '"),
    "')",
    # " LIMIT 1000", 
    ";")
  
  rm(tabela_empresas_rfb, tabela_estabelecimentos_rfb, campo_cnpj, 
     campo_natureza_juridica, natjur_nao_lucrativo)
  
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
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 1, 
    path_file_backup = ifelse(definicoes$salva_backup, path_file_backup, NULL))
  
  rm(path_file_backup)
  
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
