# Script para atualizar as informações do Mapa das Organizações da Sociedade Civil (MOSC)
# Script para incorporar os dados completos da RAIS ao MOSC

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-06-27

# To do: ####

## Verificcar erros e inconsistências nas variáveis ao longo dos anos.
## Corrigir os nomes das variáveis
## Inserir o id_osc


# Setup ####

library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)
library(assertthat)
library(readxl)

# Colocar aqui até onde retroceder nos anos de incorporação ao MOSC
PrimeiroAno <- 2010

# Parâmetro para salvar arquivos baixados na base RAIS
SaveBackup <- TRUE
UsaBackup <- TRUE

# Chaves de acesso aos dados:
ChaveRAIS <- "keys/rais_2019_MuriloJunqueira.json" # banco de dados RAIS
ChaveMOSC <- "keys/psql12-homolog_keySUPER.json" # banco de dados MOSC

# Diretório de backup do arquivos extraídos
DirEstab <- "data/raw/RAIS/estabelecimentos/"
DireVinculos <- "data/raw/RAIS/vinculos/"

# Diretório de Download dos dados
downloadDir <- "data/raw/RAIS/backup/"

# Cria o diretório de download dos dados, caso não existir:
if(!dir.exists(downloadDir)) dir.create(downloadDir)

# Dicionario de dados RAIS:
CodeBook <- read_xlsx("tab_auxiliares/NomesCampos.xlsx", 
                      sheet = "CamposRAIS")

# Unidades Federativas
UFs <- fread("tab_auxiliares/UFs.csv", encoding = "Latin-1")

# Função para facilitar a conexão de dados:
source("src/generalFunctions/postCon.R")

assert_that(exists("postCon"))
assert_that(is.function(postCon))

# Natureza Jurídica das organizações não governamentais
NatJurOSC <- c(3069, 3220, 3301, 3999)

# Conecção à chave MOSC
connecMOSC <- postCon(ChaveMOSC, "-c search_path=osc")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Incorpora Estabelecimentos ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Se houver a tabela dos estabelecimentos no MOSC, remover:
TablesMOSC <- dbListTables(connecMOSC)

if(dbExistsTable(connecMOSC, "tb_osc_estabelecimentos_rais")) {
  dbRemoveTable(connecMOSC, "tb_osc_estabelecimentos_rais")
  }

# Coneta à base RAIS
connecRAIS_Estab <- postCon(ChaveRAIS, "-c search_path=estabelecimentos")

# Baixa as tabelas de estabelecimentos
Tables_Estab <- dbListTables(connecRAIS_Estab)

# deixa as tabelas em formato de BD
# Tables <- c("tb_vinculos_2020", "tb_vinculos_2021")
ArquivosAno <- Tables_Estab %>%  
  str_subset("^tb_estabelecimentos") %>% # inicio do nome
  str_subset("\\d{4}$") %>%  # acabe com exatamente 4 dígitos
  # Transforma em tibble
  enframe(name = NULL, value = "Arquivo") %>% 
  # Cria variável Ano
  mutate(Ano = as.integer(str_sub(Arquivo, -4, -1))) %>% 
  # Ordena e filtra por ano
  arrange(Ano) %>% 
  dplyr::filter(Ano >= PrimeiroAno)

estabelecimentosDir <- paste0(downloadDir, "estabelecimentos/")

# Cria o diretório para guardar os estabelecimentos, caso não existir:
if(!dir.exists(estabelecimentosDir)) dir.create(estabelecimentosDir)

# Nomes dos Campos:
CamposEstabelecimentosOLD <- CodeBook$campo_original[
  CodeBook$schema == "estabelecimentos"]

CamposEstabelecimentosNEW <- CodeBook$campo_mosc[
  CodeBook$schema == "estabelecimentos"]

## Baixa os dados por UF e natureza jurídica - Estabelecimentos ####
for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 1
  
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    
    for (j in seq_along(NatJurOSC)) {
      # j <- 3
      
      # Nome do arquivo baixado
      NameFile <- paste0("Estabs_", ArquivosAno$Ano[h], "_", 
                         UFs$UF_Sigla[i], "_", NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", 
              str_sub(as.character(lubridate::now()), 1, 19))
      
      # Se estiver ligado "UsaBackup" e houver o arquivo backup,
      # usa dados do backup para empilhar os dados
      if(UsaBackup && 
         file.exists(paste0(estabelecimentosDir, NameFile))) {
        
        # Carrega dados do arquivo Backup
        rawData <- readRDS(paste0(estabelecimentosDir, NameFile))
        
      } else {
        
        # Realiza um teste para saber se as variáveis "uf_ipea" e "uf" estão
        # trocadas:
        teste <- dbGetQuery(connecRAIS_Estab,
                            paste0("SELECT * FROM ", ArquivosAno$Arquivo[h],
                                   " LIMIT 10000",
                                   ";"))
        
        UFVar <- ifelse(all(is.na(teste$uf_ipea)), 
                        paste0("uf = '", UFs$UF_Id[i], "'"), 
                        paste0("uf_ipea = ", UFs$UF_Id[i]))
        rm(teste)
        
        # Baixa Dados Brutos
        rawData <- dbGetQuery(connecRAIS_Estab, 
                              paste0("SELECT * FROM ", ArquivosAno$Arquivo[h], " ",
                                     "WHERE natureza_juridica = ", NatJurOSC[j],
                                     " and ", 
                                     UFVar,
                                     # " LIMIT 10000", # Baixa apenas uma amostra dos dados
                                     ";"))
        
        # Salva diretório intermediário:
        if(SaveBackup){
          saveRDS(rawData, paste0(estabelecimentosDir, NameFile))
        }
      }
      
      # Corrige nomes de variáveis
      assert_that(all(names(rawData) %in% CamposEstabelecimentosOLD))
      
      OrdemCampos <- map_int(names(rawData), function(x) {
        which(CamposEstabelecimentosOLD == x) })
      
      names(rawData) <- CamposEstabelecimentosNEW[OrdemCampos]
      
      rm(OrdemCampos)
      
      # Colocar aqui correção das variáveis
      TidyData <- rawData %>% 
        # Corrige inconsistências com a UF
        mutate(cd_uf_ipea = UFs$UF_Id[i]) %>% 
        select(everything())
      
      assert_that(class(TidyData$cd_uf_ipea) == "integer")
      
      ## Colocar aqui upload do banco:
      if(!dbExistsTable(connecMOSC, "tb_osc_estabelecimentos_rais")) {
        dbWriteTable(connecMOSC, "tb_osc_estabelecimentos_rais", TidyData)
      } else {
        dbAppendTable(connecMOSC, 
                      "tb_osc_estabelecimentos_rais", 
                      TidyData)
      }
      
      rm(rawData, TidyData, NameFile, UFVar)
      
      # Vou fazer as buscas dormirem um pouco para não ser confundido
      # com um ataque ao servidor
      Sys.sleep(sample(1:3, 1))
    }
    rm(j)
  }
  rm(i)
}
rm(h, CamposEstabelecimentosOLD, CamposEstabelecimentosNEW)

# Finaliza a parte dos Estabelecimentos da Rotina:
dbDisconnect(connecRAIS_Estab) # Desconecta da base
rm(connecRAIS_Estab, Tables_Estab, ArquivosAno, estabelecimentosDir)
ls()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Incorpora Vínculos ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Se houver a tabela dos estabelecimentos no MOSC, remover:
if(dbExistsTable(connecMOSC, "tb_osc_vinculos_rais")) {
  dbRemoveTable(connecMOSC, "tb_osc_vinculos_rais")
}

# Coneta à base dos vínculos RAIS
connecRAIS_Vinculos <- postCon(ChaveRAIS, 
                               "-c search_path=vinculos_v6")

TablesVinculos <- dbListTables(connecRAIS_Vinculos)

# deixa as tabelas em formato de BD
# Tables <- c("tb_vinculos_2020", "tb_vinculos_2021")
ArquivosAno <- TablesVinculos %>%  
  str_subset("^tb_vinculos") %>% # inicio do nome
  str_subset("\\d{4}$") %>%  # acabe com exatamente 4 dígitos
  # Transforma em tibble
  enframe(name = NULL, value = "Arquivo") %>% 
  # Cria variável Ano
  mutate(Ano = as.integer(str_sub(Arquivo, -4, -1))) %>% 
  # Ordena e filtra por ano
  arrange(Ano) %>% 
  dplyr::filter(Ano >= PrimeiroAno)

VinculosDir <- paste0(downloadDir, "vinculos/")

# Cria o diretório para guardar os estabelecimentos, caso não existir:
if(!dir.exists(VinculosDir)) dir.create(VinculosDir)

# Nomes dos Campos:
CamposVinculosOLD <- CodeBook$campo_original[CodeBook$schema == "vinculos_v6"]
CamposVinculosNEW <- CodeBook$campo_mosc[CodeBook$schema == "vinculos_v6"]

## Baixa os dados por UF e natureza jurídica - Vínculos####
for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 8
  
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    
    for (j in seq_along(NatJurOSC)) {
      # j <- 1
      
      NameFile <- paste0("Vinculos_", ArquivosAno$Ano[h], "_", 
                         UFs$UF_Sigla[i], "_", NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", 
              str_sub(as.character(lubridate::now()), 1, 19))
      
      if(UsaBackup && 
         file.exists(paste0(estabelecimentosDir, NameFile))) {
        
        # Carrega dados do arquivo Backup
        rawData <- readRDS(paste0(estabelecimentosDir, NameFile))
        
      } else {
       
        
        # Realiza um teste para saber se as variáveis "uf_ipea" e "uf" estão
        # trocadas:
        teste <- dbGetQuery(connecRAIS_Vinculos,
                            paste0("SELECT * FROM ", ArquivosAno$Arquivo[h],
                                   " LIMIT 1000",
                                   ";"))
        
        UFVar <- ifelse(all(is.na(teste$uf_ipea)), 
                        paste0("uf = '", UFs$UF_Id[i], "'"), 
                        paste0("uf_ipea = ", UFs$UF_Id[i]))
        rm(teste)
        
        # Baixa dados brutos
        rawData <- dbGetQuery(connecRAIS_Vinculos, 
                              paste0("SELECT * FROM ", ArquivosAno$Arquivo[h], " ",
                                     "WHERE natureza_juridica = ", NatJurOSC[j],
                                     " and ", UFVar,
                                     # " LIMIT 10000", # Baixa apenas uma amostra dos dados
                                     ";"))
        
        # Salva diretório intermediário:
        if(SaveBackup){
          saveRDS(rawData, paste0(VinculosDir, NameFile))
        } 
      }
      
      # Corrige nomes de variáveis
      assert_that(all(names(rawData) %in% CamposVinculosOLD))
      
      OrdemCampos <- map_int(names(rawData), function(x) {
        which(CamposVinculosOLD == x) })
      
      names(rawData) <- CamposVinculosNEW[OrdemCampos]
      rm(OrdemCampos)
      
      # Colocar aqui correção das variáveis
      TidyData <- rawData %>% 
        # Corrige inconsistências com a UF
        mutate(cd_uf_ipea = UFs$UF_Id[i]) %>% 
        select(everything())
      
      assert_that(class(TidyData$cd_uf_ipea) == "integer")
      
      
      ## Colocar aqui upload do banco:
      if(!dbExistsTable(connecMOSC, "tb_osc_vinculos_rais")) {
        dbWriteTable(connecMOSC, "tb_osc_vinculos_rais", TidyData)
      } else {
        dbAppendTable(connecMOSC, 
                      "tb_osc_vinculos_rais", 
                      TidyData)
      }
      
      rm(rawData, TidyData, NameFile, UFVar)
      
      # Vou fazer as buscas dormirem um pouco para não ser confundido
      # com um ataque ao servidor
      Sys.sleep(sample(1:3, 1))
    }
    rm(j)
  }
  rm(i)
}
rm(h, CamposVinculosOLD, CamposVinculosNEW)

# Finaliza a parte dos Vínculos da Rotina:
dbDisconnect(connecRAIS_Vinculos) # Desconecta da base de vínculos RAIS
rm(connecRAIS_Vinculos, TablesVinculos, ArquivosAno, VinculosDir)
ls()


# Finaliza Rotina: ####
dbDisconnect(connecMOSC) # Desconecta da base MOSC
rm(connecMOSC, TablesMOSC, CodeBook, UFs)
rm(ChaveMOSC, ChaveRAIS, PrimeiroAno, downloadDir, SaveBackup, UsaBackup)
rm(DirEstab, DireVinculos, NatJurOSC)
rm(postCon)
ls()

# Fim ####