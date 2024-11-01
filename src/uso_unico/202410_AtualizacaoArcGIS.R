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



input_busca_geo <- fread("data/raw/IBGE/CNEFE_ArcGIS/input_busca_geo.csv", 
                         encoding = "UTF-8")

latlonArcGIS <- readxl::read_xlsx("data/backup_files/2024_01/IntermediateFiles/mapaosc_20241029_202410291739.xlsx",
                                  sheet = "mapaosc_20241029_202410291739")

names(latlonArcGIS)

output_df <- latlonArcGIS %>% 
  select(cnpj, tx_enderec, geom, Status, Score, Match_type) %>% 
  rename(tx_endereco = tx_enderec) %>% 
  rename(cd_identificador_osc = cnpj) %>% 
  mutate(cd_identificador_osc = ifelse(cd_identificador_osc == "NA", NA, cd_identificador_osc))

sum(is.na(output_df$cd_identificador_osc))


output_df[["Longitude"]] <- output_df$geom %>% 
  str_remove(fixed("POINT (")) %>% 
  str_remove(fixed(")")) %>% 
  str_remove(" .*")

output_df[["Latitude"]] <- output_df$geom %>% 
  str_remove(fixed("POINT (")) %>% 
  str_remove(fixed(")")) %>% 
  str_extract(" .*") %>% 
  str_trim()



saveRDS(output_df, "data/backup_files/2024_01/IntermediateFiles/LatLonOSC.RDS")




# Fim