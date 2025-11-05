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
library(DBI)
library(RODBC)
library(RPostgres) 


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega Dados da RFB ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa os dados da Receita, se não tiver sido feito ainda
# "111": Processo 11 (Insere dados do quadro societário) e 1 (completo)
if( !(111 %in% processos_att_atual) ) {

  ## Checagens preliminares ####
  assert_that(
    file.exists(glue("{diretorio_att}output_files/idControl.RDS")) |
      exists("idControl"), 
    msg = "'idControl' não encontrado na memória ou em arquivo salvo!"
  )

  message(agora(), "  Carrega Dados do Quadro Societário")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  # Início do processo ####
  processos_att_atual <- c(processos_att_atual[processos_att_atual != 111], 110)
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 11, 
    processo_nome = "Insere dados do quadro societário")
  

  # Conecta à base de dados da Receita Federal:
  source("src/generalFunctions/postCon.R") 
  conexao_rfb <- postCon(definicoes$credenciais_rfb, 
                         Con_options = 
                           glue(
                             "-c search_path={definicoes$schema_receita}"))
  if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")
  rm(postCon)
  
  
  # Carrega os campos necessários para os testes abaixo.
  CamposAtualizacao <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita)
  
  tabela_empresas_rfb <- CamposAtualizacao %>% 
    dplyr::filter(campos == "tabela_empresas_rfb") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  tabela_socios_rfb <- CamposAtualizacao %>% 
    dplyr::filter(campos == "tabela_socios_rfb") %>% 
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
  
  
  query_naolucrativo_rfb <- glue(
    "SELECT * FROM {tabela_empresas_rfb} 
      RIGHT JOIN {tabela_socios_rfb} 
      ON {tabela_socios_rfb}.{campo_cnpj} \\
      = {tabela_empresas_rfb}.{campo_cnpj} 
      WHERE {campo_natureza_juridica} IN ('",
    paste(natjur_nao_lucrativo, collapse = "', '"),
    "')",
    # " LIMIT 1000", 
    ";")
  
  # query_naolucrativo_rfb
  
  rm(tabela_empresas_rfb, tabela_socios_rfb, campo_cnpj, 
     campo_natureza_juridica, natjur_nao_lucrativo)
  
  # Executa a busca do quadro societário das OSC:
  quadro_societario_raw <- dbGetQuery(conexao_rfb, query_naolucrativo_rfb)
  
  rm(query_naolucrativo_rfb)
  
  # Remove variáveis duplicadas:
  var_duplicadas <- names(quadro_societario_raw) %>% 
    enframe(name = NULL, value = "var") %>% 
    group_by(var) %>% 
    summarise(Freq = n()) %>% 
    dplyr::filter(Freq > 1) %>% 
    select(var) %>% unlist() %>% as.character()
  
  to_remove <- integer(0)
  
  for (i in seq_along(var_duplicadas) ) {
    # i <- 2
    message(var_duplicadas[i])
    to_remove <- c(to_remove, which(names(quadro_societario_raw) == var_duplicadas[i])[-1])
  }
  
  quadro_societario_clean <- quadro_societario_raw[ , -to_remove]
  
  rm(i, to_remove, var_duplicadas)
  
  rm(quadro_societario_raw)
  
  # Código da fonte de dados RFB
  FonteRFB <- paste0("CNPJ/SRF/MF/", codigo_presente_att)
  
  # Resgata os id dos quadros existentes:
  id_quadro_societario_Old <- tbl(conexao_mosc, "tb_quadro_societario") %>% 
    select(id_quadro_societario, tx_cpf_socio, tx_nome_socio) %>% 
    collect() %>% 
    mutate(tx_cpf_socio = str_pad(as.character(tx_cpf_socio), 
                                  width = 11, 
                                  side = "left", 
                                  pad = "0"), 
           # Esse truque aqui é para compensar o fato de que o CPF não está
           # completo no banco.
           id_socio_temp = paste0(tx_cpf_socio, "_", tx_nome_socio)
           ) %>% 
    select(id_socio_temp, id_quadro_societario)
  
  # Formata os dados
  tb_quadro_societario <- quadro_societario_clean %>% 
    
    mutate(cd_identificador_osc = str_pad(as.character(cnpj), 
                          width = 14,
                          side = "left",
                          pad = "0"
                          )) %>% 
    
    # Filtra apenas os CNPJ que são OSC:
    dplyr::filter(cd_identificador_osc %in% idControl$cd_identificador_osc) %>% 
    
    # Insere os 'id_osc':
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Formata dados:
    rename(
      tx_nome_socio = nome_socio,
      tx_cpf_socio = cnpj_cpf_socio
      ) %>% 
    
    mutate(
      tx_cpf_socio = str_pad(as.character(tx_cpf_socio), 
                             width = 11, 
                             side = "left", 
                             pad = "0"),
      tx_data_entrada_socio = ymd(data_entrada_sociedade),
      cd_qualificacao_socio = as.integer(qualificacao_socio),
      cd_tipo_socio = as.integer(faixa_etaria),
      ft_nome_socio = FonteRFB,
      ft_cpf_socio = FonteRFB,
      ft_data_entrada_socio = FonteRFB,
      ft_qualificacao_socio = FonteRFB,
      ft_tipo_socio = FonteRFB,
      bo_oficial = TRUE, 
      id_socio_temp = paste0(tx_cpf_socio, "_", tx_nome_socio),
      ) %>% 
    
    # Insere os 'id_quadro_societario':
    left_join(id_quadro_societario_Old, by = "id_socio_temp") %>% 
    
    # Remove a maioria dos campos da tabela 'empresas':
    select(
      -razao_social,
      -natureza_juridica,
      -qualificacao_responsavel,
      -porte_empresa,
      -ente_federativo_responsavel,
      -capital_social,
    ) %>% 
    
    # Ordem das variáveis
    select(     
      id_quadro_societario,
      id_osc,
      tx_nome_socio,
      ft_nome_socio,
      tx_cpf_socio,
      ft_cpf_socio,
      tx_data_entrada_socio,
      ft_data_entrada_socio,
      cd_qualificacao_socio,
      ft_qualificacao_socio,
      cd_tipo_socio,
      ft_tipo_socio,
      bo_oficial,
      everything()
    )
  

  
  # Estou aqui !!!! ####
  
  # Cria novos 'id_quadro_societario':
  if(sum(is.na(tb_quadro_societario$id_quadro_societario)) > 0) {
    
    # id máximo antigo
    Max_OldID <- max(id_quadro_societario_Old$id_quadro_societario, na.rm = TRUE)
    
    # Novos IDs
    NewID <- seq(from = Max_OldID + 1, 
                 to = Max_OldID + sum(is.na(tb_quadro_societario$id_quadro_societario)), 
                 by = 1)
    
    # Adiciona novos IDs
    tb_quadro_societario$id_quadro_societario[is.na(tb_quadro_societario$id_quadro_societario)] <- NewID
    
    assert_that(sum(is.na(tb_quadro_societario$id_quadro_societario)) == 0)
    
    rm(Max_OldID, NewID)
  }
  
  # Caminho do arquivo Backup
  path_file_backup <- glue("{diretorio_att}output_files/tb_quadro_societario.RDS")
  
  # Salva arquivo Backup ####
  if(definicoes$salva_backup) saveRDS(tb_quadro_societario, path_file_backup)
  
  # Atualiza controle de processos ####
  processos_att_atual <- c(processos_att_atual[processos_att_atual != 110], 111)
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 11, 
    path_file_backup = ifelse(definicoes$salva_backup, path_file_backup, NULL))
  
  rm(path_file_backup, FonteRFB, id_quadro_societario_Old)
  rm(quadro_societario_clean)
  
  dbDisconnect(conexao_rfb)
  rm(conexao_rfb)
  gc()
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("tb_quadro_societario") || 
                file.exists(
                  glue("{diretorio_att}intermediate_files/tb_quadro_societario.RDS")),
              
              msg = glue("Não foi encontrado os dados do quadro societário na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 11 consta como concluído!")) %>% 
    if(.)  message("Dados do quadro societário já processados anteriormente")
  
  }


# Fim ####
