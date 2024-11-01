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
# Extrai endereços das OSC (somente OSC novas ou que mudaram) ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito
if(!(91 %in% processos_att_atual)) {
  
  message("Extrai localização das OSC")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 91], 90))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 9, 
    processo_nome = "Extrai localização das OSC")
  
  # Se o arquivo não estiver carregado, carrega ele.
  if(!(exists("DB_OSC") && "data.frame" %in% class(DB_OSC)) ) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    # diretorio_att <- "backup_files/2024_01/"
    PathFile <- paste0(diretorio_att, "intermediate_files/DB_OSC.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(PathFile), 
                msg = paste0("Arquivo '", PathFile, "' não encontrado."))
    
    # Carrega ele
    DB_OSC <- readRDS(PathFile)
    # names(DB_OSC)
    # nrow(DB_OSC)
    rm(PathFile)
  }
  
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
                                  pad = "0"))
  
  # Carrega dados dos endereços das osc já existentes:
  tb_localizacao_old <- dbGetQuery(conexao_mosc, 
                                   glue("SELECT * FROM tb_localizacao",
                                        # " LIMIT 500", 
                                        ";"))
  
  # tb_localizacao_old <- readRDS("data/temp/tb_localizacao_old.RDS")
  
  # Carrega Dados de ID e CNPJ das OSC antigas:
  tb_osc_old <- dbGetQuery(conexao_mosc, 
                       glue("SELECT * FROM tb_osc",
                            # " LIMIT 500", 
                            ";"))

  # tb_osc_old <- readRDS("data/temp/tb_osc_old.RDS")
  
  # Adiciona CNPJ das OSC em 'tb_localizacao_old':
  tb_localizacao_old <- tb_localizacao_old %>% 
    left_join(select(tb_osc_old, id_osc, cd_identificador_osc), by = "id_osc") %>% 
    mutate(cnpj = str_pad(as.character(cd_identificador_osc), 
                          width = 14, 
                          side = "left", 
                          pad = "0"), 
           nr_cep = str_pad(as.character(nr_cep), 
                            width = 8, 
                            side = "left", 
                            pad = "0"))
  
  rm(tb_osc_old)
  
  # Evita problemas de formatação no CNPJ:
  DB_OSC <- DB_OSC %>% 
    mutate(cnpj = str_pad(as.character(cnpj), 
                          width = 14, 
                          side = "left", 
                          pad = "0"))
  

  # Os tipos precisam ser minimamente compatíveis:
  # A chance aqui é probabilística, mas a chance de dar errado é menos de um 
  # em um bilhão!
  # ( (897/879)-1 )^20 # (vezes arranjo de 3 em 20)
  assert_that(
    sum(
      tb_localizacao_old$cnpj[ sample(seq_len(nrow(tb_localizacao_old)), 20) ] %in% 
        DB_OSC$cnpj) > 3 )

  
  novos_enderecos_osc <- DB_OSC %>% 
    select(cnpj, cep, numero) %>% 
    left_join(select(tb_localizacao_old, cnpj, nr_cep, nr_localizacao), 
              by = "cnpj") %>% 
    mutate(nr_cep = str_pad(as.character(nr_cep), 
                          width = 8, 
                          side = "left", 
                          pad = "0"), 
           cep = str_pad(as.character(cep), 
                          width = 8, 
                          side = "left", 
                          pad = "0"), 
           ref_endereco_old = paste0(nr_cep, "_", nr_localizacao), 
           ref_endereco_new = paste0(cep, "_", numero), 
           flag_new = ref_endereco_old != ref_endereco_new) %>% 
    dplyr::filter(flag_new)
  
  
  source("src/generalFunctions/clean_adrs_number.R")
  
  # Insere variável de número de endereço limpa
  tb_localizacao_old$nr_clean <- clean_adrs_number(tb_localizacao_old$nr_localizacao)
  novos_enderecos_osc$nr_clean <- clean_adrs_number(novos_enderecos_osc$numero)
  
  # Cria lista de endereços aproximados em 'tb_localizacao_old':
  enderecos_existentes <- tb_localizacao_old %>% 
    select(nr_cep, nr_clean) %>% 
    mutate(numero_aproximado = ifelse(
      ( nr_clean == "SN" | nr_clean == "KM" | is.na(nr_clean) ),
                                      "SN", 
      as.character( floor(as.numeric(nr_clean)/500) ) ), 
      
      endereco_aproximado = paste0(str_sub(nr_cep, 1, 5), "-",
                                   str_sub(nr_cep, 6, 8),
                                   "_", numero_aproximado)
      ) %>% 
    distinct(endereco_aproximado)
  
  
  novos_enderecos_osc2 <- novos_enderecos_osc %>% 
    select(cnpj, nr_cep, nr_clean) %>% 
    mutate(numero_aproximado = ifelse(
      ( nr_clean == "SN" | nr_clean == "KM" | is.na(nr_clean) ),
      "SN", as.character( floor(as.numeric(nr_clean)/500) ) ), 
      
      endereco_aproximado = paste0(str_sub(nr_cep, 1, 5), "-",
                                   str_sub(nr_cep, 6, 8),
                                   "_", numero_aproximado),
      Ja_Existe = endereco_aproximado %in% enderecos_existentes[["endereco_aproximado"]]) %>% 
    dplyr::filter(!Ja_Existe)
  
  
  # Extrai as informações necessárias:
  input_busca_geo <- DB_OSC %>%
    dplyr::filter(cnpj %in% novos_enderecos_osc2$cnpj) %>% 
    # Variáveis de endereço:
    select(cnpj, razao_social, tipo_logradouro, logradouro, 
           numero, bairro, cep, municipio, pais) %>% 
    # Renomear código Municipal RFB:
    rename(CodMuniRFB = municipio) %>% 
    # Não pode ter sede no exterior nem valor nulo de município
    dplyr::filter(CodMuniRFB != "9707", 
                  !is.na(CodMuniRFB)) %>% 
    # Coloca código municipal do IBGE
    left_join(CodMunicRFB, by = "CodMuniRFB") %>% 
    # Une o endereço em um texto único:
    mutate(cep = str_pad(as.character(cep), 
                         width = 8, 
                         side = "left", 
                         pad = "0"),
           cep2 = paste0(str_sub(cep, 1, 5), "-", str_sub(cep, 6, 8)),
      tx_endereco = paste0(tipo_logradouro, " ", logradouro, ", ", numero,
                           ", BAIRRO ", bairro, ", CEP ", cep2,
                               ", ", Munic_Nome2, 
                               "-", UF),
      # Não considerar zona rural um bairro:
      tx_endereco = str_replace(tx_endereco, " BAIRRO ZONA RURAL", 
                                " ZONA RURAL"))
  
  # Analisa como ficou a formatação do endereço:
  # input_busca_geo$tx_endereco[sample(seq_len(nrow(input_busca_geo)), 10)]
  
  # Salva arquivos de endereços das OSC
  fwrite(input_busca_geo, 
         paste0(diretorio_att, "intermediate_files/input_busca_geo.csv"),
         sep = ";", dec = ",")
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 9, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              "data/raw/Galileo/GalileoINPUT.RDS", 
                              NULL))
  
    rm(input_busca_geo, novos_enderecos_osc, tb_localizacao_old)
    rm(CodMunicRFB)
    rm(clean_adrs_number, enderecos_existentes, novos_enderecos_osc2)
    # ls()
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("DB_OSC") || 
                file.exists(
                  glue("{diretorio_att}intermediate_files/DB_OSC.RDS")),
              
              msg = glue("Não foi encontrado o objeto 'DB_OSC.RDS' na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 31 consta como concluído!")) %>% 
    if(.)  message("Arquivo com exportação dos endereços das OSC feito ", 
                   "anteriormente!")
  
}


# Fim ####