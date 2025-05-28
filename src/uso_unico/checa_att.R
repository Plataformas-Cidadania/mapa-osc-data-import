# Objetivo do Script: checar e corrigir dados da atualização

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2025-05-12

library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)

# Concecta aos bancos de dados do MOSC ####

# Chaves do banco de dados
# keys <- "keys/psql12-homolog_key.json" # Banco de homologação
keys <- "keys/psql12-usr_manutencao_mapa.json" # Banco de produção

# Concecta aos bancos de dados do MOSC:
source("src/generalFunctions/postCon.R") 
conexao_mosc <- postCon(keys, 
                        Con_options = "-c search_path=osc")

dbIsValid(conexao_mosc)

rm(postCon, keys)


# Informações sobre o banco de dados  ####
tables <- dbListTables(conexao_mosc)

tables



# Total de OSCs
tb_osc <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM tb_osc",
           # " LIMIT 500",
           ";")
  )
)

names(tb_osc)


sum(tb_osc$bo_osc_ativa) # 917865

# cara, estamos com aquela divergência entre total geral (917865) e 
# somatório por região (917773)


# Número de OSC sem a informação da região

tb_localizacao <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM tb_localizacao",
           # " LIMIT 500",
           ";")
  )
)

names(tb_localizacao)


teste <- tb_localizacao %>% 
  left_join(tb_osc, by = "id_osc") %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  mutate(uf = str_sub(cd_municipio, 1, 2)) %>% 
  group_by(uf) %>% 
  summarise(Freq = n())

teste

View(teste)

sum(teste$Freq)
# 917805
# 0 == 32

917865 - 917805 # 60
917865 - 60 - 32 # 917773 (bingo!)


teste1 <- tb_localizacao %>% 
  left_join(tb_osc, by = "id_osc") %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  mutate(uf = str_sub(cd_municipio, 1, 2)) 


sum(is.na(teste1$cd_municipio))
sum(is.na(teste1$bo_osc_ativa))
nrow(teste1) # 917805


tb_dados_gerais <- try(
  dbGetQuery(
    conexao_mosc,
    paste0("SELECT * FROM tb_dados_gerais",
           # " LIMIT 500",
           ";")
  )
)

names(tb_osc)


Faltantes <- tb_osc %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  dplyr::filter(!id_osc %in% teste1$id_osc)


DadosFaltantes <- tb_dados_gerais %>% 
  dplyr::filter(id_osc %in% Faltantes$id_osc) %>% 
  left_join(
    select(tb_osc, id_osc, cd_identificador_osc), 
    by = "id_osc"
  )

DB_OSC <- readRDS("backup_files/2025_01/intermediate_files/DB_OSC.RDS")

names(DB_OSC)

DB_OSC <- DB_OSC %>% 
  mutate(
    cd_identificador_osc = str_pad(as.character(cnpj), 
                                   width = 14, 
                                   side = "left", 
                                   pad = "0")
  )

DadosFaltantes <- DadosFaltantes %>% 
  mutate(
    cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                   width = 14, 
                                   side = "left", 
                                   pad = "0")
  )

class(DB_OSC$cd_identificador_osc)

tb_osc <- tb_osc %>% 
  mutate(
    cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                   width = 14, 
                                   side = "left", 
                                   pad = "0")
  )


oscFaltantes <- DB_OSC %>% 
  dplyr::filter(cd_identificador_osc %in% DadosFaltantes$cd_identificador_osc) %>% 
  left_join(select(tb_osc, cd_identificador_osc, id_osc), 
            by = "cd_identificador_osc")

names(oscFaltantes)

View(oscFaltantes)

table(oscFaltantes$municipio)





nrow(tb_localizacao)

sum(is.na(tb_localizacao$cd_municipio))

sum(is.na(tb_localizacao$geo_localizacao))


sum(tb_localizacao$cd_municipio == "0")


1438887 %in% tb_localizacao$id_osc


teste <- DB_OSC %>% 
  
  # Insere "id_osc"
  left_join(idControl, by = "cd_identificador_osc") %>% 
  dplyr::filter(id_osc == 1438887)


1438887 %in% teste$id_osc


1438887 %in% LatLon_data$id_osc


# Extrai as informações necessárias:
input_busca_geo <- oscFaltantes %>%
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


input_busca_geo <- input_busca_geo  %>% 
  # slice(1:10000) %>% 
  mutate(logradouro_full = paste(tipo_logradouro, logradouro), 
         logradouro_full = str_remove(logradouro_full, fixed("S/N") ), 
         numero = str_squish(numero), 
         numero = str_remove(numero, " .*"), 
         numero = str_remove_all(numero, "[:alpha:]"), 
         numero = str_squish(numero), 
         numero = as.numeric(numero)    )

campos <- geocodebr::definir_campos(
  logradouro = "logradouro_full",
  numero = "numero",
  cep = "cep",
  localidade = "bairro",
  municipio = "Munic_Nome",
  estado = "UF"
)

HorarioInicio <- lubridate::now()

geo_loc <- geocodebr::geocode(
  enderecos = input_busca_geo, 
  campos_endereco = campos, 
  verboso = TRUE, 
  resolver_empates = TRUE)

HorarioFim <- lubridate::now()

message("Duração da Busca ", round(HorarioFim - HorarioInicio, 2), 
        " minutos")

message("Média de ", 
        round(lubridate::as.duration( 
          (HorarioFim - HorarioInicio)/nrow(input_busca_geo)), 
          6), 
        " segundos")

View(geo_loc)


