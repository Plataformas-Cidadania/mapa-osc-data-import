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
if( !(11 %in% processos_att_atual) ) {

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
  ## 320-1 Estabelecimento, no Brasil, de Fundação ou Associação Estrangeiras
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
    str_split(fixed("|")) %>% magrittr::extract2(1) %>% 
    paste(collapse = "', '") %>% 
    paste0("'", ., "'")
  
  # Filtros para a query da Receita:
  filtros_rfb <- CamposAtualizacao %>% 
    dplyr::filter(campos == "filtro_query_rfb") %>% 
    select(nomes) %>% unlist() %>% as.character()
    
  rm(CamposAtualizacao)
  

  
  
  # Baixa dados da RFB
  
  # Query para resgatar ao mesmo tempo dados de empresas e estabelecimentos
  # filtrando por naturezas jurídicas não lucraticas, na Receita Federal:
  
  # Baixa dados dos estabelecimentos:
  
  message("Baixa dados dos estabelecimentos")
  
  filtrorfb_1 <- filtros_rfb %>% 
    paste0(collapse = " AND ") %>% 
    paste0(" AND ")
  
  query_estabelecimentos <- glue(
    "
SELECT * FROM {tabela_empresas_rfb}
    WHERE {filtrorfb_1} 
    natureza_juridica IN ({natjur_nao_lucrativo})",
    #"\nLIMIT 200000",
    ";"
  )
  
  # query_estabelecimentos
  
  message("Início da busca: ", agora())
  empresas_df <- dbGetQuery(conexao_rfb, query_estabelecimentos)
  message("Fim da busca: ", agora())
  
  rm(filtrorfb_1, query_estabelecimentos)
  
  Sys.sleep(2)
  
  message("Baixando estabelecimentos:")
  
  
  x <- unique(empresas_df$cnpj_basico)
  chunck_size <- 50000
  
  split_CNPJ <- split(x, ceiling(seq_along(x)/chunck_size) )
  
  # length(split_CNPJ)
  rm(x, chunck_size)
  
  filtrorfb_2 <- filtros_rfb %>% 
    paste0("e.", .) %>% 
    paste0(collapse = " AND ") 
  
  
  source("src/generalFunctions/fix_duplicate_names.R")
  
  estabelecimentos_df <- tibble()
  
  message("Início da busca: ", agora())
  for (j in seq_along(split_CNPJ) ) {
    # j <- 5
    
    message(j, "/", length(split_CNPJ))
    
    x_j <- split_CNPJ[[j]] %>% 
      paste0(collapse = "', '") %>% 
      paste0("'", ., "'")
    
    query_j <- glue("
    WITH lista_procura AS (SELECT unnest(ARRAY[{x_j}]) AS cnpj)
SELECT e.* FROM {tabela_estabelecimentos_rfb} e
INNER JOIN lista_procura lp ON e.{campo_cnpj} = lp.cnpj
WHERE {filtrorfb_2};"
    )
    rm(x_j)
    # query_j
    # names(data_j)
    
    message("Início do ciclo ", j, ":   ", agora())
    data_j <- dbGetQuery(conexao_rfb, query_j)
    message("Fim do ciclo ", j, ":   ", agora())
    
    # Corrige nomes duplicados:
    data_j <- fix_duplicate_names(data_j, alert = FALSE)
    
    estabelecimentos_df <- bind_rows(estabelecimentos_df, data_j)
    rm(query_j, data_j)
  }
  rm(j, filtrorfb_2, split_CNPJ)
  
  message("Fim da busca: ", agora())
  
  rm(tabela_empresas_rfb, tabela_estabelecimentos_rfb, campo_cnpj, 
     campo_natureza_juridica, natjur_nao_lucrativo, filtros_rfb)
  
  # names(estabelecimentos_df)
  # names(empresas_df)
  
  tb_JoinOSC <- estabelecimentos_df %>% 
    select(-mes, -ano) %>% 
    left_join(empresas_df, by = c("cnpj_basico")) %>% 
    # Corrige nomes duplicados
    fix_duplicate_names()
  
  rm(estabelecimentos_df, empresas_df, fix_duplicate_names)
  # ls()
  
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
  gc()
  
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
