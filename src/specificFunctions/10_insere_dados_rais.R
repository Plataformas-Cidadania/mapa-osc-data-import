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
if( !(101 %in% processos_att_atual) & definicoes$atualiza_RAIS ) {
  
  message("Insere os dados da RAIS")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  assert_that(!is.null(definicoes$schemas_RAIS))
  assert_that(!is.null(definicoes$tabela_RAIS))
  
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 101], 100))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 10, 
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
  conexao_RAIS <- postCon(definicoes$credenciais_rfb, 
                         Con_options = 
                           glue(
                             "-c search_path={definicoes$schemas_RAIS}"))

  assert_that(dbIsValid(conexao_RAIS), 
              msg = "Não foi possível conectar ao banco 'rais_2019'") %>% 
    if(.) message("Conectado ao BD 'rais_2019'")
  
  rm(postCon)
  
  # Pega o ano máximo da tabela
  max_year <- dbGetQuery(
    conexao_RAIS, 
    glue("SELECT MAX (ano) FROM {definicoes$tabela_RAIS};")
  )
  

  # Baixa Naturezas jurídicas
  campo_naturezas_juridicas <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita) %>% 
    dplyr::filter(campos == "natjur_nao_lucarivo") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character() %>% 
    str_split(fixed("|")) %>% magrittr::extract2(1)
  
  # Baixa UFs
  UFs <- fread("tab_auxiliares/UFs.csv")
  
  # Baixa e processa dados por NatJur e UF
  
  Dados_RAIS <- tibble()
  
  for (h in seq_len(nrow(UFs)) ) {
    # h <- 8
    for (j in seq_along(campo_naturezas_juridicas)) {
      # j <- 3
      
      message("Baixando dados de ", UFs$UF_Sigla[h], 
              ", NatJur ", campo_naturezas_juridicas[j])
      
      # Baixa dados brutos
      rawData <- dbGetQuery(
        conexao_RAIS, 
        glue("SELECT * FROM {definicoes$tabela_RAIS}",
             " WHERE natureza_juridica = '{campo_naturezas_juridicas[j]}'",
             " and uf = '{UFs$UF_Sigla[h]}' and ano = {max_year}", 
             " LIMIT 10000", # Baixa apenas uma amostra dos dados
             ";"))
      
      NewRows <- rawData %>% 
        mutate(id_estab = str_pad(as.character(id_estab), 
                                  width = 14, 
                                  side = "left", 
                                  pad = "0")) %>% 
      dplyr::filter(id_estab %in% idControl$cd_identificador_osc)
      
      Dados_RAIS <- bind_rows(Dados_RAIS, NewRows)
      rm(NewRows, rawData)
    }
  }
  rm(h, j, campo_naturezas_juridicas, UFs, max_year)
  
  # Tira estatísticas por OSC
  RAIS_OSC <- Dados_RAIS %>% 
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

  # nrow(RAIS_OSC)
  # names(RAIS_OSC)
  
  # Remove caso de id_osc nulo
  # (não deve ocorrer, mas vou deixar aqui para garantir)
  RAIS_OSC <- RAIS_OSC[!is.na(RAIS_OSC$id_osc), ]
  
  # table(RAIS_OSC$ano)
  
  # Versão dos dados 1 (igual no MOSC, sem o ano)
  tb_relacoes_trabalho <- RAIS_OSC %>% 
    group_by(id_osc) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(-ano)
  
  # Versão dos dados 1 (adicionando o ano)
  tb_relacoes_trabalho2 <- RAIS_OSC %>% 
    group_by(id_osc, ano) %>% 
    slice(1) %>% 
    ungroup()
  
  # Salva Backup
  if(definicoes$salva_backup) {
    PathFile <- glue("{diretorio_att}output_files/tb_relacoes_trabalho.RDS")
    saveRDS(tb_relacoes_trabalho, PathFile)
    
    PathFile2 <- glue("{diretorio_att}output_files/tb_relacoes_trabalho2.RDS")
    saveRDS(tb_relacoes_trabalho2, PathFile2)
    
    rm(PathFile, PathFile2)
  }
  
  # Atualiza realização de processos:
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 100], 101))
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 10)
  
  # Finaliza Rotina
  dbDisconnect(conexao_RAIS)
  rm(tb_relacoes_trabalho, tb_relacoes_trabalho2, Dados_RAIS)  
  rm(RAIS_OSC, conexao_RAIS, idControl)
  # ls()
} else {
  
  if(definicoes$atualiza_RAIS) {
    message("Dados RAIS já inseridos")
  } else {
    message("Configurações programadas para não atualizar RAIS")
  }
    
}


# Fim ####