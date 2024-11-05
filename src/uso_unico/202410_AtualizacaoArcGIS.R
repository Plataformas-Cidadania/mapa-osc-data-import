# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: incorporar os dados de geolocalização do feitos com base no
# ArcGIS

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-10-31


# bibliotecas necessárias:
library(magrittr)
library(data.table)
library(dplyr)
library(glue)
library(stringr)
library(assertthat)
library(readxl)


# Defasado:
# input_busca_geo <- fread("data/raw/IBGE/CNEFE_ArcGIS/input_busca_geo.csv", 
#                          encoding = "UTF-8")
# 
# latlonArcGIS <- readxl::read_xlsx("data/backup_files/2024_01/IntermediateFiles/mapaosc_20241029_202410291739.xlsx",
#                                   sheet = "mapaosc_20241029_202410291739")


source("src/generalFunctions/postCon.R") 
assert_that(is.function(postCon))

# Concecta aos bancos de dados do MOSC:
conexao_spat <- postCon("keys/rais_2019_MuriloJunqueira.json", 
                        Con_options = "-c search_path=spat")

tables <- dbListTables(conexao_spat)

tables

latlonArcGIS <- try(dbGetQuery(conexao_spat, 
                               glue("SELECT * FROM mapaosc_202410",
                                    #" LIMIT 500", 
                                    ";")))

nrow(latlonArcGIS)

length(latlonArcGIS$cnpj) == length(unique(latlonArcGIS$cnpj))
sum(is.na(latlonArcGIS$cnpj))

names(latlonArcGIS)

output_df <- latlonArcGIS %>% 
  select(cnpj, x, y, status, score, match_type, addr_type) %>% 
  rename(cd_identificador_osc = cnpj, 
         Longitude = x, 
         Latitude = y) %>% 
  mutate(cd_identificador_osc = ifelse(cd_identificador_osc == "NA", 
                                       NA, 
                                       cd_identificador_osc))

length(output_df$cd_identificador_osc) == length(unique(output_df$cd_identificador_osc))
sum(is.na(output_df$cd_identificador_osc))

saveRDS(output_df, "backup_files/2024_01/intermediate_files/LatLonOSC.RDS")

file.exists("backup_files/2024_01/intermediate_files/LatLonOSC.RDS")

dbDisconnect(conexao_spat)

rm(postCon, tables, latlonArcGIS, conexao_spat, output_df)

# Fim