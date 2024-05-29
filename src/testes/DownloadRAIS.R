# Scrip para controlar as funções de atualização MOSC

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-05-28

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)
library(jsonlite)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa a chave secreta do código
KeyFile <- "keys/rais_2019_MuriloJunqueira.json"
assert_that(file.exists(KeyFile))
keys <- jsonlite::read_json(KeyFile)


# Verifica se pode conectar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = keys$dbname,
                            host = keys$host,
                            port = keys$port,
                            user = keys$username, 
                            password = keys$password,
                            options="-c search_path=vinculos_v6")

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = keys$dbname,
                    host = keys$host,
                    port = keys$port,
                    user = keys$username, 
                    password = keys$password,
                    options="-c search_path=vinculos_v6")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao)


ArquivosAno <- tribble(
  ~Arquivo, ~Ano, 
  "tb_vinculos_2021", 2020,
  "tb_vinculos_2021", 2021)


UFs <- tribble(
  ~UFNome, ~UF_Id, ~UF_Sigla,
  "Acre", 12, "AC",
  "Alagoas", 27, "AL",
  "Amapá", 16, "AP",
  "Amazonas", 13, "AM",
  "Bahia", 29, "BA",
  "Ceará", 23, "CE",
  "Distrito Federal", 53, "DF",
  "Espírito Santo", 32, "ES",
  "Goiás", 52, "GO",
  "Maranhão", 21, "MA",
  "Mato Grosso", 51, "MT",
  "Mato Grosso do Sul", 50, "MS",
  "Minas Gerais", 31, 'MG',
  "Pará", 15, "PA",
  "Paraíba", 25, "PB",
  "Paraná", 41, "PR",
  "Pernambuco", 26, "PE",
  "Piauí", 22, "PI",
  "Rio Grande do Norte", 24, "RN",
  "Rio Grande do Sul", 43, "RS",
  "Rio de Janeiro", 33, "RJ",
  "Rondônia", 11, "RO",
  "Roraima", 14, "RR",
  "Santa Catarina", 42, "SC",
  "São Paulo", 35, "SP",
  "Sergipe", 28, "SE",
  "Tocantins", 17, "TO")


NatJurOSC <- c(3069, 3220, 3301, 3999)


if(!dir.exists("backup_files/2023_01/input_files/RAIS")) {
  dir.create("backup_files/2023_01/input_files/RAIS")
}


for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 1
  
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    
    for (j in seq_along(NatJurOSC)) {
      # j <- 1
      
      NameFile <- paste0(ArquivosAno$Ano[h], "_", UFs$UF_Sigla[i], "_", NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", as.character(lubridate::now()))
      
      # Baixa uma amostra de 10.000 linhas.
      rawData <- dbGetQuery(connec, 
                            paste0("SELECT * FROM tb_vinculos_2021 ",
                                   "WHERE natureza_juridica = ", NatJurOSC[j],
                                   " and uf_ipea = ", UFs$UF_Id[i],
                                   " LIMIT 10000",
                                   ";"))
      
      saveRDS(rawData, paste0("backup_files/2023_01/input_files/RAIS/", NameFile))
      rm(rawData, NameFile)
    }
    rm(j)
  }
}






# filepath <- "data/temp/2024-05-22 RAIS/2024-05-22 RAIS - vinculos/tb_vinculos_2021.RDS"
# 
# file.exists(filepath)
# tb_vinculos_2021 <- readRDS(filepath)
# 
# names(tb_vinculos_2021)
