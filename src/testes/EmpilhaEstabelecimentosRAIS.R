# Scrip para Empilhar dados da RAIS Estabelecimentos

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-06-20

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)


# Empilha dados Estabelecimentos RAIS ####

# Diretório de Download
downloadDir <- "data/raw/RAIS/estabelecimentos/"

# ID das OSCs
idControl <- readRDS("tab_auxiliares/idControl.RDS")

# Lista arquivos baixados:
RawDataFiles <- list.files(downloadDir, "RDS$") %>% 
  enframe(name = NULL, value = "Arquivo") %>% 
  mutate(ano = as.integer(str_sub(Arquivo, 1, 4))) %>% 
   dplyr::filter(ano == 2020) %>% 
  arrange(ano)

# base para agregar todos os anos:
RAIS_Estabelecimentos_Data <- tibble()

# Processa arquivos
for (i in seq_along(RawDataFiles$Arquivo)) {
  # i <- 1
  
  message("Processando arquivo: ", RawDataFiles$Arquivo[i],
          " (", i, "/", nrow(RawDataFiles), ")")
  
  # Carrega dados brutos:
  rawdata <- readRDS(paste0(downloadDir, RawDataFiles$Arquivo[i]))
  
  # Processa os dados:
  newdata <- rawdata %>% 
    # Corrige o padding do CNPJ
    mutate(cnpj_cei = str_pad(as.character(cnpj_cei), width = "14", 
                              side = "left", pad = 0)) %>% 
    # Muda o nome para o padrão do MOSC:
    rename(cd_identificador_osc = cnpj_cei) %>% 
    # Adiciona Id da OSC
    left_join(idControl, by = "cd_identificador_osc") %>% 
    # Filtra as OSC do banco (que tem Id)
    dplyr::filter(!is.na(id_osc)) %>% 
    # Ordena os campos
    select(id_osc, ano, cd_identificador_osc, everything())
  
  if(i == nrow(RawDataFiles) || 
     (RawDataFiles$ano[i] != RawDataFiles$ano[i+1])) {
    
    message("Salva dados do ano de ", RawDataFiles$ano[i])
    
    # Salva arquivo
    Dumpfilename <- paste0("data/raw/RAIS/", 
                           "RAIS_Estabelecimentos_Data_", 
                           RawDataFiles$ano[i], 
                           ".RDS")
    
    saveRDS(RAIS_Estabelecimentos_Data, Dumpfilename)
    
    # Descarrega memória:
    rm(RAIS_Estabelecimentos_Data, Dumpfilename)
    RAIS_Estabelecimentos_Data <- tibble()
  } else {
    
    # Agrava novo conjunto de dados:
    RAIS_Estabelecimentos_Data <- bind_rows(RAIS_Estabelecimentos_Data, 
                                            newdata)
  }

  rm(newdata, rawdata)
}
rm(i)

# Salva arquivo
# saveRDS(RAIS_Estabelecimentos_Data, 
#         "data/raw/RAIS/RAIS_Estabelecimentos_Data.RDS")

# map_int(RAIS_Estabelecimentos_Data, function(x) sum(is.na(x)))


rm(RAIS_Estabelecimentos_Data, idControl)
rm(RawDataFiles, downloadDir)
# Fim ####