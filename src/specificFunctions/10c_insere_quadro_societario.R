# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: baixa dados brutos da Receita Federal do Brasil, já filtrando
# as organizações sem fins lucrativos.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2025-11-03


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
  
  assert_that(
    dbIsValid(conexao_mosc) && exists("conexao_mosc"), 
    msg = "A rotina não está conectada ao banco MOSC"
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
  
  # Filtros para a query da Receita:
  # filtros_rfb <- CamposAtualizacao %>% 
  #   dplyr::filter(campos == "filtro_query_rfb") %>% 
  #   select(nomes) %>% unlist() %>% as.character()
  
  rm(CamposAtualizacao, tabela_empresas_rfb,
     campo_natureza_juridica, natjur_nao_lucrativo)
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc, bo_osc_ativa) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  
  x <- unique(idControl$cd_identificador_osc) %>% 
    str_sub(1, 8)
  
  chunck_size <- 50000
  
  split_CNPJ <- split(x, ceiling(seq_along(x)/chunck_size) )
  
  # length(split_CNPJ)
  rm(x, chunck_size)
  
  # filtrorfb_2 <- filtros_rfb %>% 
  #   paste0("s.", .) %>% 
  #   paste0(collapse = " AND ") 
  
  
  filtrorfb_2 <- glue(
    "s.ano = '{year(definicoes$data_dados_referencia)}' AND ",
    "s.mes = '{month(definicoes$data_dados_referencia)}'"
  )
  
  source("src/generalFunctions/fix_duplicate_names.R")
  
  socios_df_list <- vector(mode = "list", length = length(split_CNPJ))
  
  # Executa a busca do quadro societário das OSC:
  message("Baixando dados do Quadro Societário")
  
  message("Início da busca: ", agora())
  for (j in seq_along(split_CNPJ) ) {
    # j <- 1
    
    message(j, "/", length(split_CNPJ))
    
    x_j <- split_CNPJ[[j]] %>% 
      paste0(collapse = "', '") %>% 
      paste0("'", ., "'")
    
    query_j <- glue("
    WITH lista_procura AS (SELECT unnest(ARRAY[{x_j}]) AS cnpj)
SELECT s.* FROM {tabela_socios_rfb} s
INNER JOIN lista_procura lp ON s.{campo_cnpj} = lp.cnpj
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
    
    # socios_df <- bind_rows(socios_df, data_j)
    # View(socios_df_list[[j]])
    socios_df_list[[j]] <- data_j
    rm(query_j, data_j)
  }
  message("Fim da busca: ", agora())
  rm(j, filtrorfb_2, split_CNPJ, fix_duplicate_names,
     tabela_socios_rfb, campo_cnpj)
  
  socios_df <- bind_rows(socios_df_list)
  
  rm(socios_df_list)
  
  # query_naolucrativo_rfb
  
  message("Formata dados do Quadro Societário")
  
  # Código da fonte de dados RFB
  FonteRFB <- paste0("CNPJ/SRF/MF/", codigo_presente_att)
  
  # Resgata os id dos quadros existentes:
  id_quadro_societario_Old <- tbl(conexao_mosc, "tb_quadro_societario") %>% 
    select(id_quadro_societario, id_osc, tx_cpf_socio, tx_nome_socio) %>% 
    collect() %>% 
    mutate(tx_cpf_socio = str_pad(as.character(tx_cpf_socio), 
                                  width = 11, 
                                  side = "left", 
                                  pad = "0"), 
           # Esse truque aqui é para compensar o fato de que o CPF não está
           # completo no banco.
           id_socio_temp = paste0(id_osc, "_", tx_cpf_socio, "_", tx_nome_socio)
           ) %>% 
    # Para evitar relações "many-to-many":
    distinct(id_socio_temp, .keep_all = TRUE) %>% 
    select(id_socio_temp, id_quadro_societario)

  # Prepara idControl com cnpj_basico (8 primeiros dígitos do CNPJ) para o join:
  # A relação entre socios_df e idControl é many-to-many: um sócio pode estar em
  # várias OSC, e cada OSC pode ter vários estabelecimentos 
  # (cd_identificador_osc)com o mesmo cnpj_basico. O quadro societário é
  # replicado para cada estabelecimento da organização.
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc, bo_osc_ativa) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  idControl_basico <- idControl %>%
    mutate(cnpj_basico = str_sub(cd_identificador_osc, 1, 8)) %>% 
    dplyr::filter(bo_osc_ativa) %>% 
    select(-bo_osc_ativa)
  
  rm(idControl)
  
  # socios_df %>% 
  #   group_by(cnpj_basico) %>% 
  #   summarise(Count_freq = n()) %>% 
  #   group_by(Count_freq) %>% 
  #   summarise(Freq = n()) %>% 
  #   print(n = 50)
  # 
  # nrow(tb_quadro_societario)

  # nrow(tb_quadro_societario) / nrow(idControl_basico)
  
  # Formata os dados
  tb_quadro_societario <- socios_df %>% 
    
    # Mantém apenas as colunas necessárias para reduzir uso de memória:
    select(
      cnpj_basico,
      nome_socio,
      cnpj_cpf_socio,
      data_entrada_sociedade,
      qualificacao_socio,
      identificador_de_socio
    ) %>%
    
    # Filtra apenas os CNPJ básicos que pertencem a OSC:
    dplyr::filter(cnpj_basico %in% idControl_basico$cnpj_basico) %>% 
    
    # Remove duplicatas de sócio por cnpj_basico antes do join:
    # socios_df tem uma linha por (CNPJ completo, sócio), então o mesmo sócio
    # aparece N vezes se a empresa tem N estabelecimentos na RFB. Sem essa
    # deduplicação, o join many-to-many gera N×M linhas por sócio (onde M é
    # o número de estabelecimentos em idControl_basico), causando explosão.
    distinct(cnpj_basico, cnpj_cpf_socio, nome_socio, .keep_all = TRUE) %>%
    
    # Replica o quadro societário para cada estabelecimento 
    # (cd_identificador_osc) com o mesmo cnpj_basico em idControl
    # (relação many-to-many):
    left_join(idControl_basico, 
              by = "cnpj_basico", 
              relationship = "many-to-many") %>%

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
      cd_tipo_socio = as.integer(identificador_de_socio),
      ft_nome_socio = FonteRFB,
      ft_cpf_socio = FonteRFB,
      ft_data_entrada_socio = FonteRFB,
      ft_qualificacao_socio = FonteRFB,
      ft_tipo_socio = FonteRFB,
      bo_oficial = TRUE,
      id_socio_temp = paste0(id_osc, "_", tx_cpf_socio, "_", tx_nome_socio),
      ) %>%

    # Evita duplicação de sócios na mesma OSC (mesmo id_osc):
    distinct(id_socio_temp, .keep_all = TRUE) %>%

    # Insere os 'id_quadro_societario' (resgata ids antigos onde disponível):
    left_join(id_quadro_societario_Old, by = "id_socio_temp") %>%

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
      cd_identificador_osc
    )
  rm(socios_df, FonteRFB, idControl_basico)
  
  # names(tb_quadro_societario)
  # View(tb_quadro_societario)
  # sum(is.na(tb_quadro_societario$id_quadro_societario))
  
  # Cria novos 'id_quadro_societario' (inclusive para linhas duplicadas por
  # estabelecimento que não existiam no banco anterior):
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
  rm(id_quadro_societario_Old)
  
  # Garante consistência das chaves primárias:
  source("src/generalFunctions/Check_PK_Rules.R")
  
  Check_PK_Rules(tb_quadro_societario$id_quadro_societario)
  rm(Check_PK_Rules)
  
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
  
  rm(path_file_backup, tb_quadro_societario)
  # ls()
  
  dbDisconnect(conexao_rfb)
  rm(conexao_rfb)
  gc()
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("tb_quadro_societario") || 
                file.exists(
                  glue("{diretorio_att}output_files/tb_quadro_societario.RDS")),
              
              msg = glue("Não foi encontrado os dados do quadro societário na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 11 consta como concluído!")) %>% 
    if(.)  message("Dados do quadro societário já processados anteriormente")
  
  }


# Fim ####
