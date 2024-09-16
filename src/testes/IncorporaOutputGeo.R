# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: formatar o output do GALILEO para poder gerar as
# variáveis de geolocalização do MOSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-08-01


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes

## Funções auxiliares:

## Outputs:


# bibliotecas necessárias:


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# .... ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito
if(!(91 %in% processos_att_atual)) {
  
  message("Extrai localização das OSC")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  input_file <- glue("{diretorio_att}input_files/ExtractGalileo.xlsx")
  
  # Verifica se o input do Galileo Existe:
  assert_that(file.exists(input_file))
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 91], 90))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 9, 
    processo_nome = "Extrai localização das OSC")
  
  Galileo_Raw <- read_excel(input_file, 
                            sheet = 1)
  
  # names(Galileo_Raw)
  # class(Galileo_Raw$cd_identificador_osc)
  # class(OSC_Suplementar$cd_identificador_osc)
  
  OSC_Suplementar <- OSC_Suplementar %>% 
    mutate(New = !(cd_identificador_osc %in% Galileo_Raw$cd_identificador_osc), 
           tx_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0")) %>% 
    select(id_osc, tx_identificador_osc, 
           Longitude, Latitude, PrecisionDepth)
  
  # names(OSC_Suplementar)
  
  Galileo <- Galileo_Raw %>% 
    mutate(Flag = tx_identificador_osc %in% OSC_Suplementar[["tx_identificador_osc"]], 
           tx_identificador_osc = str_pad(as.character(tx_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0")) %>% 
    dplyr::filter(!Flag) %>%
    select(id_osc, tx_identificador_osc, 
           Longitude, Latitude, PrecisionDepth) %>% 
    bind_rows(OSC_Suplementar) %>% 
    dplyr::filter(!is.na(PrecisionDepth),
                  Longitude != 0, 
                  Latitude != 0, 
                  !is.na(Longitude), 
                  !is.na(Latitude)) %>% 
    #group_by(tx_identificador_osc) %>% 
    #slice(1) %>% 
    #ungroup() %>% 
    mutate(PrecisionDepth = str_remove(PrecisionDepth, "Estrelas"), 
           PrecisionDepth = str_remove(PrecisionDepth, "Estrela"), 
           PrecisionDepth = str_trim(PrecisionDepth),
           PrecisionDepth = as.integer(PrecisionDepth)) %>% 
    rename(cd_identificador_osc = tx_identificador_osc, 
           cd_precisao_localizacao = PrecisionDepth)
  
  sum(is.na(Galileo$PrecisionDepth))
  
  saveRDS(Galileo, "data/raw/Galileo/GalileoINPUT.RDS")
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 8, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              "data/raw/Galileo/GalileoINPUT.RDS", 
                              NULL))
  
  
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