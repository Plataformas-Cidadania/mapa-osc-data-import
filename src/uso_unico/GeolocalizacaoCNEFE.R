# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: testar uma forma de criar um sistema de geolocalização
# usando dados do IBGE-CNEFE

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-09-27


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs: Arquivos compactados do CNEFE/IBGE, por UF.

## Funções auxiliares: (nenhuma)

## Outputs: arquivos RDS com resumos das informações geográficas:
# <uf_cod>_<uf_sigle>_munic_geo: Centroide dos endereços do municípios
# <uf_cod>_<uf_sigle>_cep_geo:  Centroide do CEP
# <uf_cod>_<uf_sigle>_rua_geo: Grafia correta e centroide das ruas
# <uf_cod>_<uf_sigle>_endereco_geo: latlon de cada endereço

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(readxl)
library(utils) # para unzip

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria BD de localizações do CNEFE ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Expressões de Correção de Endereços:
tb_corrige_enderecos <- read_xlsx("tab_auxiliares/tb_corrige_enderecos.xlsx", 1)

# Função Para Corrigir Endereços:
source("src/generalFunctions/uniformiza_enderecos.R")

# Seleção das colunas relevantes do input_file:
selColunas <- list(character = c(3, 9, 11, 12, 13),
                   integer = c(14),
                   numeric = c(26, 27))

# Coloca o nome das colunas relevantes do input_file:
coluna_nomes <- c("COD_MUNICIPIO", "CEP", "NOM_TIPO_SEGLOGR", 
                  "NOM_TITULO_SEGLOGR", "NOM_SEGLOGR", "NUM_ENDERECO", 
                  "LATITUDE", "LONGITUDE")

# Máximo de linhas por chunk do input_file:
max_rows <- 1000000

input_folder <- "data/raw/IBGE/CNEFE/"
output_folder <- "tab_auxiliares/latlon_enderecos/"

# Lista de arquivos compactados do CNEFE/IBGE, por UF.
uf_files <- list.files(input_folder, pattern = ".zip$")

# Processa cada um dos arquivos compactados:
for (i in seq_along(uf_files)) {
  # i <- 8
  uf <- str_remove(uf_files[i], fixed(".zip"))
  
  message("Lendo dados de ", uf)
  
  # Se o arquivo já foi processado, pular:
  if( file.exists( glue("{output_folder}{uf}_endereco_geo.RDS") ) ) {
    message("Arquivo já processado")
    rm(uf)
    next
  }
  
  message("Descompactando o arquivo zip")
  
  # Descompacta o arquivo
  zip_file <- glue("{input_folder}{uf}.zip")
  unzip(zip_file, exdir = "data/raw/IBGE/CNEFE/")
  
  # Arquivo CSV:
  input_file <- glue("{input_folder}{uf}.csv")
  assert_that(file.exists(input_file))
  
  message("Lendo arquivo de dados")
  
  # Calcula o número de linhas do input_file
  nlinhas <- readr::count_fields(
    input_file, 
    tokenizer = readr::tokenizer_csv() ) %>% 
    length() %>% 
    magrittr::subtract(1)
  
  # Número de chunks de processamento (evita processar aquivos muito grandes)
  n_chunk <- nlinhas %/% max_rows + ifelse(nlinhas %% max_rows == 0, 0, 1)
  
  # Controle dos chunks
  chunks_df <- tibble(.rows = n_chunk) %>% 
    mutate(row_number = row_number(),
           n_rows = ifelse(row_number == max(row_number), 
                           nlinhas %% max_rows, max_rows),
           skip = (row_number - 1) * max_rows + 1)
  
  # Bases geográficas  
  munic_geo <- tibble() # Centroide dos endereços do municípios
  cep_geo <- tibble() # Centroide do CEP
  rua_geo <- tibble() # Grafia correta e centroide das ruas
  endereco_geo <- tibble() # latlon de cada endereço

  for( j in seq_len(n_chunk) ) {
    # j <- 1
    message("Processando dados do chunk ", j, " de ", n_chunk)
    
    # Carrega um chunk por vez:
    data_chunk <- fread(input_file, 
                        nrows = chunks_df$n_rows[j], 
                        skip = chunks_df$skip[j], 
                        select = selColunas) %>% 
      magrittr::set_names(coluna_nomes)
    
    # Agrega os dados por endereço:
    agrega_enderecos <- data_chunk %>% 
      group_by(COD_MUNICIPIO, CEP, NOM_TIPO_SEGLOGR, NOM_TITULO_SEGLOGR, 
               NOM_SEGLOGR, NUM_ENDERECO) %>% 
      summarise(LATITUDE = mean(LATITUDE, na.rm = TRUE), 
                LONGITUDE = mean(LONGITUDE, na.rm = TRUE)) %>% 
      ungroup()
    
    rm(data_chunk) # já vai limpando a memória assim que possível.
    
    # Cria variável de endereços corrigidos:
    message("Limpando Endereços")
    agrega_enderecos$CleanAdress <- uniformiza_enderecos(agrega_enderecos$NOM_SEGLOGR, 
                                                         tb_corrige_enderecos
                                                         , verbose = TRUE
                                                         )
    
    message("Agregando dados por endereço")
    
    # Dados por rua
    rua_geo_j <- agrega_enderecos %>% 
      group_by(COD_MUNICIPIO, CEP, NOM_TIPO_SEGLOGR, NOM_TITULO_SEGLOGR, 
               NOM_SEGLOGR, CleanAdress) %>% 
      summarise(n = n(), 
                LATITUDE = mean(LATITUDE, na.rm = TRUE), 
                LONGITUDE = mean(LONGITUDE, na.rm = TRUE)) %>% 
      ungroup() %>%
      # Endereço completo: (tipo, título [São, Dr, Comendador etc], logradouro)
      mutate(Endereco = str_trim(paste0(NOM_TIPO_SEGLOGR, " ", 
                                        CleanAdress)),
             
             # Formata Endereço:
             ## Remove acentos:
             Endereco = stringi::stri_trans_general(Endereco, 
                                                    id = "Latin-ASCII"),  
             
             ## Remove espaços excessivos:
             Endereco = str_squish(Endereco),
             
             # Id do Endereço:
             id = str_remove_all(Endereco, "[:punct:]"), # Remove pontuação
             
             # id: <CEP>_<Endereço>
             id = trim(paste0(CEP, "_", id)))

    # Dados por endereço
    endereco_geo_j <- agrega_enderecos %>% 
      mutate(Endereco = str_trim(paste0(NOM_TIPO_SEGLOGR, " ", 
                                        CleanAdress)), 
             
             # Formata Endereço:
             ## Remove acentos:
             Endereco = stringi::stri_trans_general(Endereco, 
                                                    id = "Latin-ASCII"),
             
             ## Remove espaços excessivos:
             Endereco = str_squish(Endereco),
             
             # id do Endereço:
             id = str_remove_all(Endereco, "[:punct:]"),
             id = trim(paste0(CEP, "_", id)) ) %>% 
      # Deixa apenas os dados necessários, pois o resto pode ser resgatado acima:
      select(id, COD_MUNICIPIO, NUM_ENDERECO, LATITUDE, LONGITUDE)
    
    # Centroide dos endereços dos municípios:
    munic_geo_j <- agrega_enderecos %>% 
      group_by(COD_MUNICIPIO) %>% 
      summarise(n = n(),
                LATITUDE = mean(LATITUDE, na.rm = TRUE), 
                LONGITUDE = mean(LONGITUDE, na.rm = TRUE)) %>% 
      ungroup()
    
    # Centroide dos CEPs dos municípios:
    cep_geo_j <- agrega_enderecos %>% 
      group_by(COD_MUNICIPIO, CEP) %>% 
      summarise(n = n(),
                LATITUDE = mean(LATITUDE, na.rm = TRUE), 
                LONGITUDE = mean(LONGITUDE, na.rm = TRUE)) %>% 
      ungroup()
    
    rm(agrega_enderecos)
    
    # Agrega os dados do chunck às bases de dados principais:
    munic_geo <- bind_rows(munic_geo, munic_geo_j)
    cep_geo <- bind_rows(cep_geo, cep_geo_j)
    rua_geo <- bind_rows(rua_geo, rua_geo_j)
    endereco_geo <- bind_rows(endereco_geo, endereco_geo_j)
    
    # Limpa a memória:
    rm(munic_geo_j, cep_geo_j, rua_geo_j, endereco_geo_j)
    gc() 
  }
  rm(j)
  
  message("Finalizando processamento de ", uf)
  
  # Evita possíveis duplicações de endereços:
  # (Isso é necessário, pois pode ser que um endereço ficou entre um 
  # chunck e outro)
  
  munic_geo <- munic_geo %>% 
    group_by(COD_MUNICIPIO) %>% 
    summarise(LATITUDE = weighted.mean(LATITUDE, n, na.rm = TRUE), 
              LONGITUDE = weighted.mean(LONGITUDE, n, na.rm = TRUE)) %>% 
    ungroup() %>% 
    arrange(COD_MUNICIPIO)
  
  cep_geo <- cep_geo %>% 
    group_by(COD_MUNICIPIO, CEP) %>% 
    summarise(LATITUDE = weighted.mean(LATITUDE, n, na.rm = TRUE), 
              LONGITUDE = weighted.mean(LONGITUDE, n, na.rm = TRUE)) %>% 
    ungroup() %>% 
    arrange(CEP)
  
  rua_geo <- rua_geo %>% 
    group_by(id, COD_MUNICIPIO, CEP, Endereco, NOM_TIPO_SEGLOGR, 
             NOM_TITULO_SEGLOGR, NOM_SEGLOGR, CleanAdress) %>% 
    summarise(LATITUDE = weighted.mean(LATITUDE, n, na.rm = TRUE), 
              LONGITUDE = weighted.mean(LONGITUDE, n, na.rm = TRUE)) %>% 
    ungroup() %>% 
    arrange(COD_MUNICIPIO)
  
  
  # Para 'endereco_geo', fiz uma optimização
  
  # Extrai as linhas diplicadas usando 'duplicated', de 'data.table' 
  dtdf <- data.table(endereco_geo)
  duplicated_rows1 <- duplicated(dtdf, by= c("id", "NUM_ENDERECO")) # segunda linha duplicada
  duplicated_rows2 <- duplicated(dtdf, by= c("id", "NUM_ENDERECO"),  # primeira linha duplicada 
                                 fromLast = TRUE)
  
  duplicated_rows_all <- duplicated_rows1 | duplicated_rows2 # todas as duplicadas
  
  rm(dtdf, duplicated_rows1, duplicated_rows2)

  # Debug:
  # endereco_geo <-  endereco_geo %>%
  #   arrange(id, NUM_ENDERECO)
  # sum(duplicated_rows_all)
  # endereco_geo[which(duplicated_rows_all)[1:20],]
  
  unique_endereco_geo <- endereco_geo[ !duplicated_rows_all , ]
  
  duplicated_endereco_geo <- endereco_geo[ duplicated_rows_all , ] %>% 
    group_by(COD_MUNICIPIO, id, NUM_ENDERECO) %>% 
    summarise(LATITUDE = mean(LATITUDE, na.rm = TRUE), 
              LONGITUDE = mean(LONGITUDE, na.rm = TRUE)) %>% 
    ungroup()
  
  endereco_geo <-  bind_rows(unique_endereco_geo, duplicated_endereco_geo) %>% 
    arrange(id)
  
  rm(unique_endereco_geo, duplicated_endereco_geo)
  
  # Salva os dados:
  saveRDS(munic_geo, glue("{output_folder}{uf}_munic_geo.RDS"))
  saveRDS(cep_geo, glue("{output_folder}{uf}_cep_geo.RDS"))
  saveRDS(rua_geo, glue("{output_folder}{uf}_rua_geo.RDS"))
  saveRDS(endereco_geo, glue("{output_folder}{uf}_endereco_geo.RDS"))
  
  # Deleta o arquivo descompactado:
  unlink(glue("{input_folder}{uf}.csv"))
  
  # Limpa a memória
  rm(munic_geo, cep_geo, rua_geo, endereco_geo)
  rm(uf, input_file, zip_file)
  rm(n_chunk, chunks_df, nlinhas)
  gc()
}
rm(i)

# Finaliza a rotina:
rm(input_folder, output_folder, uf_files, max_rows)
rm(coluna_nomes, selColunas)

# Fim ####