# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: com base nos dados extraídos da Receita Federal, já passada a 
# de identificação das OSC (com o find_OSC) e determinação da área de atuação,
# extrair as principais tabelas do Mapa das Organizações Da Sociedade Civil:

# tb_osc
# tb_dados_gerais
# tb_contatos
# tb_localizacao
# tb_areas_atuacao

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# DB_OSC  (objeto na memódia ou arquivo em "{diretorio_att}intermediate_files")
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
# Desmembramento da base RFB ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Desmembra todos os dados da Receita Federal nas tabelas específicas 
# do MOSC

# Executa o processo se não foi feito anteriormente
# "61": Processo 6 (Desmembramento da base RFB) e 1 (completo)
if(!(61 %in% processos_att_atual)) {
  
  message("Desmembramento da base RFB")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  assert_that(file.exists(glue("{diretorio_att}intermediate_files/LatLonOSC.RDS")),
              msg = glue("Arquivo de geolocalização ", 
                         "'intermediate_files/LatLonOSC.RDS' não está presente"))
  
  source("src/generalFunctions/Check_PK_Rules.R")
  source("src/generalFunctions/Empty2NA.R")
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 61], 60))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 6, 
    processo_nome = "Desmembramento da base RFB")
  
  # Se o arquivo não estiver carregado, carrega ele.
  if(!(exists("DB_OSC") && "data.frame" %in% class(DB_OSC)) ) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    path_file_backup <- glue("{diretorio_att}intermediate_files/DB_OSC.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(path_file_backup), 
                msg = paste0("Arquivo '", path_file_backup, "' não encontrado."))
    
    # Carrega os dados:
    DB_OSC <- readRDS(path_file_backup)
    # names(DB_OSC)
    # nrow(DB_OSC)
    rm(path_file_backup)
  }
  
  # Código da fonte de dados RFB
  FonteRFB <- paste0("CNPJ/SRF/MF/", codigo_presente_att)
  
  # Formata o CNPJ, usado aqui como chave-primária das OSC:
  DB_OSC <- DB_OSC %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cnpj), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  # Tabela: tb_osc ####
  
  message("Gerando tabela: tb_osc")
  
  # Resgata o "id_osc" antigo

  # Formata base:
  tb_osc <- DB_OSC %>% 
    rename(tx_apelido_osc = nome_fantasia) %>% 
    
    # Resgata o id_osc antigo.
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(ft_apelido_osc = FonteRFB, 
           ft_identificador_osc = FonteRFB,
           ft_osc_ativa = FonteRFB, 
           bo_osc_ativa = TRUE) %>% 
    
    arrange(id_osc) %>% 
    select(id_osc, tx_apelido_osc, ft_apelido_osc, cd_identificador_osc,
           ft_identificador_osc, bo_osc_ativa, ft_osc_ativa, cd_situacao_cadastral) %>% 
    # Evitar dar fonte de dado missing:
    mutate(ft_apelido_osc = ifelse(is.na(tx_apelido_osc), NA, ft_apelido_osc),
           ft_identificador_osc = ifelse(is.na(cd_identificador_osc), NA, ft_identificador_osc),
           ft_osc_ativa = ifelse(is.na(bo_osc_ativa), NA, ft_osc_ativa))
  
  # Cria id_osc dos novos CNPJs:
  if(sum(is.na(tb_osc$id_osc)) > 0) {
    # id máximo antigo
    Max_OldID <- max(tb_osc$id_osc, na.rm = TRUE)
    
    # Novos IDs
    NewID <- seq(from = Max_OldID + 1, 
                 to = Max_OldID + sum(is.na(tb_osc$id_osc)), 
                 by = 1)
    
    # Adiciona novos IDs
    tb_osc$id_osc[is.na(tb_osc$id_osc)] <- NewID
    
    assert_that(sum(is.na(tb_osc$id_osc)) == 0)
    
    # atualiza idControl
    idControl <- tb_osc %>% 
      select(id_osc, cd_identificador_osc) %>% 
      mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                            width = 14, 
                                            side = "left", 
                                            pad = "0"))
    
    rm(Max_OldID, NewID)
  }
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_osc$id_osc)
  
  # Remove strings vazias ("")
  tb_osc <- Empty2NA(tb_osc)
  
  # Por enquanto, vou anular os valores de tx_apelido_osc, até entender
  # melhor essa variável # TO DO
  tb_osc[["tx_apelido_osc"]] <- NA_character_
  tb_osc[["ft_apelido_osc"]] <- NA_character_
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_osc[["cd_identificador_osc"]] <- as.numeric(tb_osc[["cd_identificador_osc"]])
  
  # Salva Backup:
  if(definicoes$salva_backup) {
    
    path_file <- glue("{diretorio_att}output_files/tb_osc.RDS")
    saveRDS(tb_osc, path_file)
    
    # Atualiza controle de processos (tb_backups_files)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    rm(path_file)
  }
  
  
  
  # Tabela: tb_dados_gerais ####
  
  message("Gerando tabela: tb_dados_gerais")
  
  tb_dados_gerais <- DB_OSC %>% 
    rename(cd_natureza_juridica_osc = natureza_juridica, 
           tx_razao_social_osc = razao_social, 
           tx_nome_fantasia_osc = nome_fantasia, 
           cd_matriz_filial = matriz_filial, # No outro banco o nome do campo era 'identificador_matrizfilial ' # TO DO: uniformizar nomes dos campos
           ) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(
      # uniformizar os campos da RFB logo no início da atualização # TO DO
      # dt_fundacao_osc = as.character(ymd(data_de_inicio_atividade)),
      # dt_ano_cadastro_cnpj = as.character(ymd(data_de_inicio_atividade)), 
      
      tx_nome_fantasia_osc = ifelse(tx_nome_fantasia_osc == "", NA, tx_nome_fantasia_osc), 
      dt_fundacao_osc = as.character(ymd(data_inicio_atividades)),
      dt_ano_cadastro_cnpj = as.character(ymd(data_inicio_atividades)),
      cd_classe_atividade_economica_osc = str_sub(cnae, 1, 5), # verificar se é isso mesmo!
      ft_natureza_juridica_osc = FonteRFB, 
      ft_razao_social_osc = FonteRFB, 
      ft_nome_fantasia_osc = FonteRFB, 
      ft_fundacao_osc = FonteRFB,
      ft_ano_cadastro_cnpj = FonteRFB,
      ft_classe_atividade_economica_osc = FonteRFB,
      ft_fechamento_osc = FonteRFB, 
      data_situacao_cadastral = ymd(data_situacao_cadastral),
      cd_cnae_secundaria = str_replace_all(cnae_fiscal_secundaria, 
                                               ",", fixed(" | ")),
      
      # Evitar dar fonte de dado missing:
      ft_natureza_juridica_osc = ifelse(is.na(cd_natureza_juridica_osc), NA, ft_natureza_juridica_osc),
      ft_razao_social_osc = ifelse(is.na(tx_razao_social_osc), NA, ft_razao_social_osc),
      ft_nome_fantasia_osc = ifelse(is.na(tx_nome_fantasia_osc), NA, ft_nome_fantasia_osc),
      ft_fundacao_osc = ifelse(is.na(dt_fundacao_osc), NA, ft_fundacao_osc),
      ft_ano_cadastro_cnpj = ifelse(is.na(dt_ano_cadastro_cnpj), NA, ft_ano_cadastro_cnpj),
      ft_classe_atividade_economica_osc = ifelse(is.na(cd_classe_atividade_economica_osc), NA, ft_classe_atividade_economica_osc),
      ft_fechamento_osc = ifelse(is.na(dt_fechamento_osc), NA, ft_fechamento_osc)
    ) %>% 
    
    select(id_osc, cd_identificador_osc, cd_natureza_juridica_osc, ft_natureza_juridica_osc, 
           tx_razao_social_osc, ft_razao_social_osc, tx_nome_fantasia_osc, 
           ft_nome_fantasia_osc, dt_fundacao_osc, ft_fundacao_osc, 
           dt_ano_cadastro_cnpj, ft_ano_cadastro_cnpj, 
           dt_fechamento_osc, nr_ano_fechamento_osc, ft_fechamento_osc,
           cd_classe_atividade_economica_osc, cd_cnae_secundaria, 
           ft_classe_atividade_economica_osc, 
           cd_matriz_filial, data_situacao_cadastral)
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_dados_gerais$id_osc)
  
  # Remove strings vazias ("")
  tb_dados_gerais <- Empty2NA(tb_dados_gerais)
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_dados_gerais[["cd_identificador_osc"]] <- as.numeric(tb_dados_gerais[["cd_identificador_osc"]])
  
  
  # Salva Backup:
  if(definicoes$salva_backup) {
    
    path_file <- glue("{diretorio_att}output_files/tb_dados_gerais.RDS")
    saveRDS(tb_dados_gerais, path_file)
    
    # Atualiza controle de processos (tb_processos_atualizacao)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    rm(path_file)
  }
  
  # Tabela: tb_contato ####
  
  message("Gerando tabela: tb_contato")
  
  tb_contato <- DB_OSC %>% 
    rename(tx_email = correio_eletronico) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(telefone1 = ifelse(telefone1 == "", NA, telefone1), 
           
           tx_telefone = ifelse(is.na(telefone1), NA, 
                                paste0(ddd1, " ", telefone1)),
           
           tx_email = ifelse(tx_email == "", NA, tx_email), 
           
           # Uniformizar campos # TO DO
           # tx_telefone = ifelse(is.na(telefone_1), NA, 
           #                           paste0(ddd_1, " ", telefone_1)),
           
           ft_telefone = FonteRFB, 
           ft_email = FonteRFB,
           bo_nao_possui_email = is.na(tx_email)) %>% 
    select(id_osc, tx_telefone, ft_telefone, tx_email, ft_email,
           bo_nao_possui_email) %>% 
    # Evita linhas que não tem informação.
    dplyr::filter(!(is.na(tx_email) & is.na(tx_telefone))) %>% 
    # Evitar dar fonte de dado missing:
    mutate(ft_telefone = ifelse(is.na(tx_telefone), NA, ft_telefone),
           ft_email = ifelse(is.na(tx_email), NA, ft_email))
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_contato$id_osc)
  
  # Remove strings vazias ("")
  tb_contato <- Empty2NA(tb_contato)
  
  # Colocar contato no formato curto.
  
  tb_contato2 <- DB_OSC %>% 
    rename(tx_email = correio_eletronico) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Uniformizar campos # TO DO
    # mutate(tx_telefone = ifelse(is.na(telefone_1), NA, 
    #                             paste0(ddd_1, " ", telefone_1))
    mutate(tx_telefone = ifelse(is.na(telefone1), NA, 
                                paste0(ddd1, " ", telefone1))
    ) %>% 
    
    select(id_osc, tx_telefone, tx_email) %>% 
    gather(key = tx_tipo_contato, value = tx_contato, tx_telefone, tx_email) %>% 
    arrange(id_osc) %>% 
    mutate(ft_contato = FonteRFB, 
           tx_tipo_contato = str_remove(tx_tipo_contato, "tx_")) %>% 
    
    # Evita linhas que não tem informação.
    dplyr::filter(!is.na(tx_contato))
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(paste(tb_contato2$id_osc, tb_contato2$tx_tipo_contato))
  
  # Remove strings vazias ("")
  tb_contato2 <- Empty2NA(tb_contato2)
  
  # Salva Backup:
  if(definicoes$salva_backup) {
    
    path_file <- glue("{diretorio_att}output_files/tb_contato.RDS")
    saveRDS(tb_contato, path_file)
    
    # Atualiza controle de processos (tb_processos_atualizacao)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    path_file <- glue("{diretorio_att}output_files/tb_contato2.RDS")
    saveRDS(tb_contato2, path_file)
    
    # Atualiza controle de processos (tb_processos_atualizacao)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    rm(path_file)
  }
  
  # Tabela: tb_localizacao ####
  
  message("Gerando tabela: tb_contato")
  
  # Dados de geolocalização:
  LatLon_file <- glue("{diretorio_att}intermediate_files/LatLonOSC.RDS")
  
  assert_that(file.exists(LatLon_file))
  
  Dt_LatLon_data <- file.info(LatLon_file) %>% 
    mutate(ctime = as.character(ctime)) %>% 
    select(ctime) %>% 
    unlist() %>% 
    as.character() %>% ymd_hms()
  
  LatLon_data <- readRDS(LatLon_file)

  
  # names(LatLon_data)
  
  LatLon_data <- LatLon_data %>% 
    # Uniformiza o campo CPF:
    mutate(cd_identificador_osc = str_pad(as.character(cnpj), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"), 
           qualidade_classificacao = paste0(tipo_resultado, "_", precisao)
           ) %>% 
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    rename(Longitude = lon, 
           Latitude = lat) %>% 
    select(id_osc, Longitude, Latitude, qualidade_classificacao)
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(LatLon_data$cd_identificador_osc)
  
  # sum(is.na(LatLon_data$id_osc)) # Ver o caso dessas OSC sem CNPJ # TO DO
  
  # Tabela com o de/para do código municipal da Receita para o
  # código IBGE.
  CodMunicRFB <- fread("tab_auxiliares/CodMunicRFB.csv", 
                       encoding = "Latin-1") %>% 
    # Formata campos
    mutate(CodMuniRFB = str_pad(as.character(CodMuniRFB), 
                                width = 4, 
                                side = "left", 
                                pad = "0"),
           CodMunicIBGE = str_pad(as.character(CodMunicIBGE), 
                                  width = 7, 
                                  side = "left", 
                                  pad = "0"), 
           # Processa OSC com sede no exterior
           CodMunicIBGE = ifelse(CodMuniRFB == "9707", 0, CodMunicIBGE)
           )
  
  # Formata dados de localização das OSCs
  tb_localizacao <- DB_OSC %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Somente atualizar OSC novas ou que alteraram o endereço
    dplyr::filter(id_osc %in% LatLon_data$id_osc) %>% 
    
    rename(nr_localizacao = numero,
           tx_endereco_complemento = complemento,
           tx_bairro = bairro,
           CodMuniRFB = municipio,
           nr_cep = cep) %>% 
    
    # Não pode ter valor nulo de município
    dplyr::filter(!is.na(CodMuniRFB)) %>% 
    
    # Coloca código municipal do IBGE
    left_join(CodMunicRFB, by = ("CodMuniRFB")) %>% 
    
    # Incorpora dados de geolocalização
    left_join(LatLon_data, by = c("id_osc")) %>% 
    
    rename(tx_longitude = Longitude, 
           tx_latitude = Latitude, 
           cd_municipio = CodMunicIBGE) %>% 
    
    mutate(
      
      geo_localizacao = paste0("POINT (", tx_longitude, " ", tx_latitude, ")"),
      geo_localizacao = ifelse(is.na(tx_latitude) | is.na(tx_longitude), 
                               NA, geo_localizacao),
      
      # tornar essa fonte dinâmica, de acordo com a atualização! # TO DO
      ft_latlon = "pacote R geocodebr", 
      ft_geo_localizacao = "pacote R geocodebr",
      
      # Classificação do GALILEO:
      # qualidade_classificacao = case_when(cd_precisao_localizacao == 1 ~ "1 Estrela", 
      #                                     cd_precisao_localizacao > 1 ~ paste(cd_precisao_localizacao, "Estrelas"),
      #                                     TRUE ~ NA),
      
      # Classificação do ArcGIS
      # qualidade_classificacao = paste0(status, "_", score, "_ ", match_type, 
      #                                  "_", addr_type),
      
      dt_geocodificacao = Dt_LatLon_data, 
      # tx_endereco = paste0(tipo_de_logradouro, " ", logradouro), # Uniformizar campor # TO DO
      tx_endereco = paste0(tipo_logradouro, " ", logradouro),
      tx_endereco_corrigido2 = paste0(Munic_Nome2, ", ", UF),
      tx_endereco_corrigido = paste0(tx_endereco, ", ", nr_localizacao, ", ",
                                     ifelse(!is.na(tx_endereco_complemento), 
                                            paste0(tx_endereco_complemento, ", "), 
                                            ""), 
                                     tx_bairro, ", ", Munic_Nome2, ", ", 
                                     UF, ", ", nr_cep),
      ft_endereco_corrigido =  FonteRFB,
      ft_data_geocodificacao = FonteRFB,
      ft_endereco = FonteRFB,
      ft_localizacao = FonteRFB,
      tx_endereco_complemento = str_trim(tx_endereco_complemento), 
      ft_endereco_complemento = FonteRFB,
      ft_bairro = FonteRFB,
      ft_municipio = FonteRFB,
      ft_cep = FonteRFB,
      bo_oficial = TRUE, 
      cd_fonte_geocodificacao = NA, 
      tx_bairro_encontrado = NA, 
      ft_bairro_encontrado = NA) %>% 
    
    select(id_osc, cd_identificador_osc, tx_endereco, ft_endereco,
           nr_localizacao, ft_localizacao, tx_endereco_complemento,
           ft_endereco_complemento, tx_bairro, ft_bairro, cd_municipio,
           ft_municipio, geo_localizacao, ft_geo_localizacao,
           tx_endereco_corrigido, ft_endereco_corrigido,
           nr_cep, ft_cep, 
           tx_latitude, tx_longitude,
           dt_geocodificacao, ft_data_geocodificacao, 
           bo_oficial, qualidade_classificacao, 
           tx_endereco_corrigido2, cd_fonte_geocodificacao, 
           tx_bairro_encontrado, ft_bairro_encontrado) %>% 
    
    # Evitar dar fonte de dado missing:
    mutate(ft_endereco = ifelse(is.na(tx_endereco), NA, ft_endereco),
           ft_data_geocodificacao = ifelse(is.na(geo_localizacao), NA, ft_data_geocodificacao),
           ft_endereco_corrigido = ifelse(is.na(tx_endereco_corrigido), NA, ft_endereco_corrigido),
           ft_geo_localizacao = ifelse(is.na(geo_localizacao), NA, ft_geo_localizacao),
           ft_localizacao = ifelse(is.na(nr_localizacao), NA, ft_localizacao),
           ft_endereco_complemento = ifelse(is.na(tx_endereco_complemento), NA, ft_endereco_complemento),
           ft_bairro = ifelse(is.na(tx_bairro), NA, ft_bairro),
           ft_municipio = ifelse(is.na(cd_municipio), NA, ft_municipio),
           ft_cep = ifelse(is.na(nr_cep), NA, ft_cep))
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_localizacao$id_osc)
  
  # Remove strings vazias ("")
  tb_localizacao <- Empty2NA(tb_localizacao)
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_localizacao[["cd_identificador_osc"]] <- as.numeric(tb_localizacao[["cd_identificador_osc"]])
  tb_localizacao[["cd_municipio"]] <- as.numeric(tb_localizacao[["cd_municipio"]])
  tb_localizacao[["nr_cep"]] <- as.numeric(tb_localizacao[["nr_cep"]])
  
  # Restrições de "cd_municipio": não pode ter nulo
  assert_that(sum(is.na(tb_localizacao[["cd_municipio"]])) == 0, 
              msg = "Valores nulos encontrados em 'cd_municipio'")
  
  # Salva Backup:
  if(definicoes$salva_backup) {
    
    path_file <- glue("{diretorio_att}output_files/tb_localizacao.RDS")
    saveRDS(tb_localizacao, path_file)
    
    # Atualiza controle de processos (tb_processos_atualizacao)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    rm(path_file)
  }
  
  
  
  # Tabela: tb_area_atuacao ####
  
  message("Gerando tabela: tb_area_atuacao")
  
  tb_area_atuacao <- DB_OSC %>% 
    dplyr::filter(!is.na(micro_area_atuacao) & !is.na(macro_area_atuacao)) %>% 
    rename(tx_area_atuacao = macro_area_atuacao, 
           tx_subarea_atuacao = micro_area_atuacao) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Nome da fonte da área de atuação receita
    mutate(ft_area_atuacao = paste0("AreaAtuacaoOSC.R_", 
                                    codigo_presente_att), 
           ft_area_atuacaoPadronizado = "CNPJ/RFB",
           bo_oficial = TRUE) %>% 
    
    select(id_osc, cd_identificador_osc, tx_area_atuacao, 
           tx_subarea_atuacao, ft_area_atuacao, bo_oficial, 
           ft_area_atuacaoPadronizado) 
  
  
  fontes_alterativas_atuacao <- list.files("input_files_next", 
                                           "^tb_area_atuacao|^tb_certificado")
  
  for (g in seq_along(fontes_alterativas_atuacao)) {
    # g <- 1
    
    message("Inserindo ", fontes_alterativas_atuacao[g])

    encodguess <- glue("input_files_next/{fontes_alterativas_atuacao[g]}") %>% 
      guess_encoding() %>% 
      magrittr::extract2(1) %>% 
      magrittr::extract2(1) %>%
      magrittr::equals(c("ISO-8859-2", "ISO-8859-1", "Latin-1")) %>% 
      any() %>% 
      ifelse("Latin-1", 'UTF-8')

    Input_data <- fread(
      glue( "input_files_next/{fontes_alterativas_atuacao[g]}"), 
      encoding = encodguess)
    
    assert_that(
      all(
        c("tx_area_atuacao", "tx_subarea_atuacao", "ft_area_atuacao",
          "cd_identificador_osc") %in% names(Input_data)
      ))
    
    Input_data$tx_subarea_atuacao <- as.character(Input_data$tx_subarea_atuacao)
    
    # names(Input_data)
    
    newRows <- Input_data %>% 
      mutate(
        ft_area_atuacaoPadronizado = ft_area_atuacao,
        ft_area_atuacao = paste0(ft_area_atuacao, "/", codigo_presente_att),
        bo_oficial = TRUE, 
        cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                       width = 14,
                                       side = "left", 
                                       pad = "0")
        ) %>%
      Input_data <- fread(
        glue( "input_files_next/{fontes_alterativas_atuacao[g]}")
        )

      # Insere "id_osc"
      left_join(idControl, by = "cd_identificador_osc") %>%
      dplyr::filter(!is.na(id_osc)) %>% # Evita id NA
      distinct(id_osc, .keep_all = TRUE) %>% # Evita duplicação dos campos
      

      select(id_osc, cd_identificador_osc, tx_area_atuacao,
             tx_subarea_atuacao, ft_area_atuacao, bo_oficial,
             ft_area_atuacaoPadronizado)
    
    tb_area_atuacao <- tb_area_atuacao %>%
      bind_rows(newRows) %>%
      arrange(id_osc)
    
    # table(tb_area_atuacao$ft_area_atuacao)
    
    # Transfere arquivo de "input_files_next" para "diretorio_input_att"
    file.rename(
      from = glue( "input_files_next/{fontes_alterativas_atuacao[g]}"), 
      to = glue("{diretorio_att}input_files/{fontes_alterativas_atuacao[g]}")
    )
    
    rm(Input_data, newRows)
    rm(encodguess)
  }
  rm(g, fontes_alterativas_atuacao)

      Input_data$tx_subarea_atuacao <- as.character(Input_data$tx_subarea_atuacao)

      # names(Input_data)
      
      newRows <- Input_data %>% 
        
        mutate(
          ft_area_atuacaoPadronizado = ft_area_atuacao,
          ft_area_atuacao = paste0(ft_area_atuacao, "/", codigo_presente_att),
          bo_oficial = TRUE, 
          cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                         width = 14,
                                         side = "left", 
                                         pad = "0")
        ) %>%
        
        # Insere "id_osc"
        left_join(idControl, by = "cd_identificador_osc") %>%
        dplyr::filter(!is.na(id_osc)) %>% # Evita id NA
        distinct(id_osc, .keep_all = TRUE) %>% # Evita duplicação dos campos
        
        select(id_osc, cd_identificador_osc, tx_area_atuacao,
               tx_subarea_atuacao, ft_area_atuacao, bo_oficial,
               ft_area_atuacaoPadronizado)

      tb_area_atuacao <- tb_area_atuacao %>%
        bind_rows(newRows) %>%
        arrange(id_osc)
      
      # Transfere arquivo de "input_files_next" para "diretorio_input_att"
      file.rename(
        from = glue( "input_files_next/{fontes_alterativas_atuacao[g]}"), 
        to = glue("{diretorio_att}input_files/{fontes_alterativas_atuacao[g]}")
      )
      
      # table(tb_area_atuacao$ft_area_atuacao)
      rm(Input_data, newRows)
  }
  rm(fontes_alterativas_atuacao, g)
  
>>>>>>> Stashed changes
  tb_area_atuacao <- tb_area_atuacao %>%
    dplyr::filter(!is.na(id_osc)) # Evita id NA
  
  
  # Insere os códigos das áreas
  
  ## Tabelas auxiliares:
  dc_area_atuacao <- fread("tab_auxiliares/dc_area_atuacao.csv", encoding = "Latin-1")
  dc_subarea_atuacao <- fread("tab_auxiliares/dc_subarea_atuacao.csv", encoding = "Latin-1")
  
  # Formata dados:
  dc_area_atuacao$tx_area_atuacao <- dc_area_atuacao$tx_area_atuacao %>% 
    toupper() %>% 
    stringi::stri_trans_general(id = "Latin-ASCII") %>% 
    str_remove_all("[:punct:]") %>% 
    str_trim() %>% 
    str_squish()
  
  dc_subarea_atuacao$tx_subarea_atuacao <- dc_subarea_atuacao$tx_subarea_atuacao %>% 
    toupper() %>% 
    stringi::stri_trans_general(id = "Latin-ASCII") %>% 
    str_remove_all("[:punct:]") %>% 
    str_trim() %>% 
    str_squish()
  
  tb_area_atuacao$tx_area_atuacao <- tb_area_atuacao$tx_area_atuacao %>% 
    toupper() %>% 
    stringi::stri_trans_general(id = "Latin-ASCII") %>% 
    str_remove_all("[:punct:]") %>% 
    str_trim() %>% 
    str_squish()
  
  tb_area_atuacao$tx_subarea_atuacao <- tb_area_atuacao$tx_subarea_atuacao %>% 
    toupper() %>% 
    stringi::stri_trans_general(id = "Latin-ASCII") %>% 
    str_remove_all("[:punct:]") %>% 
    str_trim() %>% 
    str_squish()
  
  # Transforma nomes em códigos
  tb_area_atuacao <- tb_area_atuacao %>% 
    left_join(dc_area_atuacao, by = "tx_area_atuacao") %>% 
    left_join(select(dc_subarea_atuacao, -cd_area_atuacao), 
              by = "tx_subarea_atuacao") %>% 
    select(id_osc, cd_area_atuacao, cd_subarea_atuacao,
           ft_area_atuacao, bo_oficial, 
           ft_area_atuacaoPadronizado)
  
  rm(dc_area_atuacao, dc_subarea_atuacao)
  
  # Remove duplicatas da chave "id_osc & cd_area_atuacao & cd_subarea_atuacao"
  tb_area_atuacao <- tb_area_atuacao %>% 
    # Agrupa pela chave múltipla
    group_by(id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
    # Ordena pela área de atuação (para evitar que a ordem importe)
    arrange(ft_area_atuacao) %>% 
    # Mescla as diferentes fontes dos dados:
    mutate(ft_area_atuacao = paste0(unique(ft_area_atuacao), 
                                    collapse = ", "), 
           ft_area_atuacaoPadronizado = paste0(unique(ft_area_atuacaoPadronizado), 
                                               collapse = ", ")) %>% 
    # Remove as duplicatas
    distinct(id_osc, cd_area_atuacao, cd_subarea_atuacao, 
             .keep_all = TRUE) %>% 
    # Finaliza
    ungroup() %>% 
    select(everything())
  
  # table(tb_area_atuacao$ft_area_atuacao)
  
  # Adiciona novos IDs:
  
  Control_Id_AreaAtuacao <- tbl(conexao_mosc, "tb_area_atuacao") %>% 
    select(id_area_atuacao, id_osc, cd_area_atuacao, cd_subarea_atuacao) %>% 
    collect()
  
  # Tabela para iserir os Ids
  tb_area_atuacao <- tb_area_atuacao %>% 
    # Insere os controle das últimas atualizações
    left_join(Control_Id_AreaAtuacao, 
              by = c("id_osc", "cd_area_atuacao",
                     "cd_subarea_atuacao")) %>% 
    select(everything())
  
  # sum(is.na(tb_area_atuacao$id_area_atuacao))
  
  # Coloca novos IDs:
  if(sum(is.na(tb_area_atuacao$id_area_atuacao)) > 0) {
    
    # Linhas que são missing:
    MissID <- is.na(tb_area_atuacao$id_area_atuacao)
    
    # Cria novos Ids
    NewID <- seq_len(sum(MissID)) + max(Control_Id_AreaAtuacao$id_area_atuacao)
    
    # Insere novos Ids
    tb_area_atuacao$id_area_atuacao[MissID] <- NewID
    # sum(is.na(tb_area_atuacao$id_area_atuacao))

    rm(MissID, NewID)
  }
  
  # Checa Chaves Primárias únicas e não nulas:
  Check_PK_Rules(tb_area_atuacao$id_area_atuacao)
  
  # Remove strings vazias ("")
  tb_area_atuacao <- Empty2NA(tb_area_atuacao)
  
  # Finaliza o banco
  tb_area_atuacao <- tb_area_atuacao %>% 
    select(id_area_atuacao, id_osc, cd_area_atuacao, cd_subarea_atuacao,
           ft_area_atuacao, bo_oficial, ft_area_atuacaoPadronizado)
  
  rm(Control_Id_AreaAtuacao)
  
  # Salva Backup:
  if(definicoes$salva_backup) {
    
    path_file <- glue("{diretorio_att}output_files/tb_area_atuacao.RDS")
    saveRDS(tb_area_atuacao, path_file)
    
    # Atualiza controle de processos (tb_processos_atualizacao)  
    if(!definicoes$att_teste) atualiza_processos_att(
      TipoAtt = "arquivo backup", 
      id_att = id_presente_att, 
      id_processo = 6, 
      path_file_backup = ifelse(definicoes$salva_backup, 
                                path_file, 
                                NULL))
    
    rm(path_file)
  }
  
  # Atualiza realização de processos:
  
  # Atualiza controle de processos ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 60], 61))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 6)
  
  rm(FonteRFB)
  rm(DB_OSC)
  gc()
  # rm(tb_osc, tb_dados_gerais, tb_contato, tb_contato2, 
  #    tb_localizacao, tb_area_atuacao)
  # ls()
  
  } else {
    
    files <- list.files(glue("{diretorio_att}output_files/"), "^tb_") %>% 
      str_remove(fixed(".RDS"))
    memoryOBJ <-  c(str_subset(ls(), "^tb_"), files) %>% unique()
    
    tablesRFB <- c("tb_area_atuacao", "tb_contato", "tb_dados_gerais",
                   "tb_localizacao", "tb_osc")
    
    # Caso o processo já tenha sido feito anteriormente ####
    assert_that(all(tablesRFB %in% memoryOBJ),
                
                msg = glue("Não foi encontrado o arquivo ou objetos: ", 
                           tablesRFB[!tablesRFB %in% memoryOBJ],
                           "de atualização. Verifique porque o processo estão ",
                           "constanto como concluído na ausência destas tabelas")
                ) %>% 
      if(.)  message("Desmembramento da base RFB já feito anteriormente")
  
    rm(tablesRFB, files, memoryOBJ)
    
    Sys.sleep(1)
    
  }

# Fim ####