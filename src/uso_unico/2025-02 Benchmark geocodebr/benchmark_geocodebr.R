# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Fazer um benchmark sobre a performance e a precisão
# do pacote 'geocodebr', criado pelo IPEA

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2025-02-13

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# install.packages("remotes")
# remotes::install_github("ipeaGIT/geocodebr")

library(geocodebr)

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
# Benchmark ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Inicia a conexão com o bd portal_osc (se necessário):
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))){
  source("src/generalFunctions/postCon.R") 
  # Concecta aos bancos de dados do MOSC:
  conexao_mosc <- postCon("keys/psql12-homolog_key.json", 
                          Con_options = "-c search_path=osc")
  rm(postCon)
}

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

tb_localizacao_old <- dbGetQuery(conexao_mosc, 
                                 glue("SELECT * FROM tb_localizacao",
                                      # " LIMIT 500", 
                                      ";"))

amostra_benchmark <- tb_localizacao_old %>% 
  slice(
    sample(
      seq_len(nrow(tb_localizacao_old)), 
      300
    )
  ) %>% 
  dplyr::filter(ft_geo_localizacao == "Software Galileo") %>% 
  slice(1:200)

# Insere a tabela no banco de dados para processar o campo geometry
if(dbExistsTable(conexao_mosc, "teste")) try(dbRemoveTable(conexao_mosc, "teste"))
try(dbWriteTable(conexao_mosc, "teste", amostra_benchmark))

query_AddCol <- glue("ALTER TABLE teste \n",
                       " ADD COLUMN X numeric, \n", 
                       " ADD COLUMN Y numeric ",
                       ";")

query_AddCol

dbExecute(conexao_mosc, query_AddCol)

query_SetGeom <- glue("UPDATE teste \n",
                     " SET X = public.ST_X(geo_localizacao), \n", 
                     "     Y = public.ST_Y(geo_localizacao)", 
                     ";")
query_SetGeom

dbExecute(conexao_mosc, query_SetGeom)

amostra_benchmark2 <- dbGetQuery(conexao_mosc, 
                                 glue("SELECT * FROM teste",
                                      # " LIMIT 500", 
                                      ";"))

if(dbExistsTable(conexao_mosc, "teste")) try(dbRemoveTable(conexao_mosc, "teste"))

names(amostra_benchmark2)
names(CodMunicRFB)

amostra_benchmark2 <- amostra_benchmark2 %>% 
  select(tx_endereco, nr_localizacao, tx_bairro, nr_cep, cd_municipio, 
         qualidade_classificacao, x, y) %>% 
  mutate(cd_municipio = str_pad(as.character(cd_municipio), 
                                width = 7, 
                                side = "left", 
                                pad = "0")) %>% 
  left_join(select(CodMunicRFB, CodMunicIBGE, Munic_Nome, UF), 
            by = c("cd_municipio" = "CodMunicIBGE"))


campos <- geocodebr::definir_campos(
  logradouro = "tx_endereco",
  numero = "nr_localizacao",
  cep = "nr_cep",
  localidade = "tx_bairro",
  municipio = "Munic_Nome",
  estado = "UF"
)

geo_loc <- geocodebr::geocode(
  enderecos = amostra_benchmark2, 
  campos_endereco = campos, 
  verboso = TRUE)

names(amostra_benchmark)

amostra_benchmark <- amostra_benchmark %>% 
  select()


for (i in seq_len(nrow(amostra_benchmark2))[100:200]) {
  
  print(i)
  geo_loc <- try(
    geocodebr::geocode(
    enderecos = slice(amostra_benchmark2, i), 
    campos_endereco = campos, 
    verboso = TRUE)
    )
  
}



amostra_benchmark3 <- amostra_benchmark2 %>% 
  slice(-c(62, 86, 118, 129, 154))

HorarioInicio <- lubridate::now()
geo_loc <- geocodebr::geocode(
  enderecos = amostra_benchmark3, 
  campos_endereco = campos, 
  verboso = TRUE)
HorarioFim <- lubridate::now()

amostra_benchmark4 <- amostra_benchmark2 %>% 
  slice(c(62, 86, 118, 129, 154))


?slice

TempoBusca <- HorarioFim - HorarioInicio

MediaBusca <- as.duration(TempoBusca/nrow(amostra_benchmark3))

names(geo_loc)

install.packages("geosphere")
library(geosphere)

distGeo(c(0,0),c(90,90))

?geosphere::distGeo()

geosphere::distGeo()

resultado <- geo_loc %>% 
  mutate(diffLat = y - lat,
         diffLon = x - lon) %>% 
  rowwise() %>% 
  mutate(Distance = distGeo( c(x, y), c(lon, lat) ) ) %>% 
  select(diffLat, diffLon, Distance, y, x, lat,  lon,
    everything())


resultado$Distance[3]

saveRDS(resultado, "development_zone/2025-02 Benchmark geocodebr/resultado.RDS")

resultado <- readRDS("development_zone/2025-02 Benchmark geocodebr/resultado.RDS")

summary(resultado$Distance)

hist(resultado$Distance)

hist(resultado$Distance[resultado$Distance < 10000])

hist(resultado$Distance[resultado$Distance < 1000])


# Tempo para processar 100 endereços: 59 segundos

# Tempo para processar 1000 endereços: 2 minutos

# Tempo para processar 10000 endereços: 4.5 minutos

# Tempo para processar 65 mil endereços: 9,5 minutos

# Novas OSC na última atualização 24749


# N de numeros inválidos:

invalida_n_df <- DB_OSC  %>% 
  # slice(1:10000) %>% 
  mutate(numero = str_squish(numero), 
         numero = str_remove(numero, " .*"), 
         numero = str_remove_all(numero, "[:alpha:]"), 
         numero = str_squish(numero), 
         numero = as.numeric(numero)    )

sum(is.na(invalida_n_df$numero))

sum(is.na(invalida_n_df$numero)) / nrow(invalida_n_df)

# Porcentagem de números inválidos: 0.2804541

# ℹ Padronizando endereços de entrada
# Error in is.null(chamada_upstream) || as.character(chamada_upstream[[1]]) !=  : 
#   'length = 3' in coercion to 'logical(1)'





# Fim ####