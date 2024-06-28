# Script para atualizar as informações do Mapa das Organizações da Sociedade Civil (MOSC)
# Script para incorporar os dados completos da RAIS ao MOSC

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-06-27

# Setup ####

library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)
library(assertthat)

# Colocar aqui até onde retroceder nos anos de incorporação ao MOSC
PrimeiroAno <- 2010

# Parâmetro para salvar arquivos baixados na base RAIS
SaveBackup <- TRUE

# Chaves de acesso aos dados:
ChaveRAIS <- "keys/rais_2019_MuriloJunqueira.json" # banco de dados RAIS
ChaveMOSC <- "keys/localhost_key.json" # banco de dados MOSC

# Diretório de backup do arquivos extraídos
DirEstab <- "data/raw/RAIS/estabelecimentos/"
DireVinculos <- "data/raw/RAIS/vinculos/"

# Diretório de Download dos dados
downloadDir <- "data/raw/RAIS/RAIS/"

# Cria o diretório de download dos dados, caso não existir:
if(!dir.exists(downloadDir)) dir.create(downloadDir)

# Dicionario de dados RAIS:
CodeBook <- fread("data/temp/CamposRAIS.csv")

# Unidades Federativas
UFs <- fread("tab_auxiliares/UFs.csv", encoding = "Latin-1")

# Função para facilitar a conexão de dados:
source("src/generalFunctions/postCon.R")

# Natureza Jurídica das organizações não governamentais
NatJurOSC <- c(3069, 3220, 3301, 3999)

# Conecção à chave MOSC
connecMOSC <- postCon(ChaveMOSC, "-c search_path=osc")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Incorpora Estabelecimentos ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Cria se houver a tabela dos estabelecimentos no MOSC, remover:
TablesMOSC <- dbListTables(connecMOSC)

if(dbExistsTable(connec, "tb_osc_estabelecimentos_rais")) {
  dbRemoveTable(connec, "tb_osc_estabelecimentos_rais")
  }

# Coneta à base RAIS
connecRAIS_Estab <- postCon(ChaveRAIS, "-c search_path=estabelecimentos")

# Baixa as tabelas de estabelecimentos
Tables <- dbListTables(connecRAIS_Estab)

# deixa as tabelas em formato de BD
# Tables <- c("tb_vinculos_2020", "tb_vinculos_2021")
ArquivosAno <- Tables %>%  
  enframe(name = NULL, value = "Arquivo") %>% 
  mutate(Ano = as.integer(str_sub(Arquivo, -4, -1)))

estabelecimentosDir <- paste0(downloadDir, "estabelecimentos/")

# Cria o diretório para guardar os estabelecimentos, caso não existir:
if(!dir.exists(estabelecimentosDir)) dir.create(estabelecimentosDir)

# Baixa os dados por UF e natureza jurídica
for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 1
  
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    
    for (j in seq_along(NatJurOSC)) {
      # j <- 1
      
      NameFile <- paste0("Estabs_", ArquivosAno$Ano[h], "_", 
                         UFs$UF_Sigla[i], "_", NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", as.character(lubridate::now()))
      
      # Baixa dados brutos
      rawData <- dbGetQuery(connecRAIS_Estab, 
                            paste0("SELECT * FROM ", ArquivosAno$Arquivo[h], " ",
                                   "WHERE natureza_juridica = ", NatJurOSC[j],
                                   " and uf_ipea = ", UFs$UF_Id[i],
                                   # " LIMIT 10000", # Baixa apenas uma amostra dos dados
                                   ";"))
      
      # Salva diretório intermediário:
      if(SaveBackup){
        saveRDS(rawData, paste0(downloadDir, NameFile))
      }
      
      # Colocar aqui correção das variáveis ####
      
      # Colocar aqui upload do banco ####
      
      
      rm(rawData, NameFile)
    }
    rm(j)
  }
}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Incorpora Vínculos ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Coneta à base RAIS
connecRAIS_Vinculos <- postCon(ChaveRAIS, 
                               "-c search_path=estabelecimentos")

# Baixa as tabelas de estabelecimentos
Tables <- dbListTables(connecRAIS_Vinculos)

# deixa as tabelas em formato de BD
# Tables <- c("tb_vinculos_2020", "tb_vinculos_2021")
ArquivosAno <- Tables %>%  
  enframe(name = NULL, value = "Arquivo") %>% 
  mutate(Ano = as.integer(str_sub(Arquivo, -4, -1)))

estabelecimentosDir <- paste0(downloadDir, "vinculos/")

# Cria o diretório para guardar os estabelecimentos, caso não existir:
if(!dir.exists(estabelecimentosDir)) dir.create(estabelecimentosDir)

# Baixa os dados por UF e natureza jurídica
for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 1
  
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    
    for (j in seq_along(NatJurOSC)) {
      # j <- 1
      
      NameFile <- paste0("Estabs_", ArquivosAno$Ano[h], "_", 
                         UFs$UF_Sigla[i], "_", NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", as.character(lubridate::now()))
      
      # Baixa dados brutos
      rawData <- dbGetQuery(connecRAIS, 
                            paste0("SELECT * FROM ", ArquivosAno$Arquivo[h], " ",
                                   "WHERE natureza_juridica = ", NatJurOSC[j],
                                   " and uf_ipea = ", UFs$UF_Id[i],
                                   # " LIMIT 10000", # Baixa apenas uma amostra dos dados
                                   ";"))
      
      # Salva diretório intermediário:
      if(SaveBackup){
        saveRDS(rawData, paste0(downloadDir, NameFile))
      }
      
      # Colocar aqui correção das variáveis ####
      
      # Colocar aqui upload do banco ####
      
      rm(rawData, NameFile)
    }
    rm(j)
  }
}

# Empilha dados dos Estabelecimentos:



"localhost_key.json"


# Fim ####