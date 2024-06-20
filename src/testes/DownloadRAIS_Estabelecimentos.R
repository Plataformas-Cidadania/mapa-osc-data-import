# Scrip para extrair dados dos Estabelecimentos RAIS dos BD IPEA

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-06-20

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
                            options="-c search_path=estabelecimentos")

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
                    options="-c search_path=estabelecimentos")

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

Tables <- dbListTables(connec) %>% sort()

rm(keys, TestConexao, KeyFile)

# Download dos dados: ####

# Lista de bases anuais da RAIS:
ArquivosAno <- str_subset(Tables, "^tb_estabelecimentos_") %>% 
  # retirar apenas dados após 2010
  enframe(name = NULL, value = "Arquivo") %>% 
  mutate(Ano = as.integer(str_sub(Arquivo, -4, -1))) %>% 
  dplyr::filter(Ano >= 2010) %>% 
  # dplyr::filter(Ano == 2017) %>% 
  arrange(Ano, Arquivo) %>% 
  select(everything())

# Lista de UFs
UFs <- fread("tab_auxiliares/UFs.csv", encoding = "Latin-1")

# Lista de Naturezas Jurídicas
NatJurOSC <- c(3069, 3220, 3301, 3999)

# Diretório de Download
downloadDir <- "data/raw/RAIS/estabelecimentos/"

# Cria o diretório de Download, se ele não existir
if(!dir.exists(downloadDir)) dir.create(downloadDir)

# Extrai todos os dados
for (h in seq_len(nrow(ArquivosAno))) {
  # h <- 1
  for (i in seq_len(nrow(UFs))) {
    # i <- 1
    for (j in seq_along(NatJurOSC)) {
      # j <- 4
      
      # Nome do Arquivo de Destino:
      NameFile <- paste0(ArquivosAno$Ano[h], "_", UFs$UF_Sigla[i], "_", 
                         NatJurOSC[j], ".RDS")
      
      message("Arquivo: ", NameFile, " ", as.character(lubridate::now()))
      
      # Realiza um teste para saber se as variáveis "uf_ipea" e "uf" estão
      # trocadas:
      teste <- dbGetQuery(connec,
                          paste0("SELECT * FROM ", ArquivosAno$Arquivo[h],
                                 " LIMIT 10000",
                                 ";"))
      
      UFVar <- ifelse(all(is.na(teste$uf_ipea)), 
                      paste0("uf = '", UFs$UF_Id[i], "'"), 
                      paste0("uf_ipea = ", UFs$UF_Id[i]))
      
      rm(teste)
      
      # Baixa Dados Brutos
      rawData <- dbGetQuery(connec, 
                            paste0("SELECT * FROM ", ArquivosAno$Arquivo[h], " ",
                                   "WHERE natureza_juridica = ", NatJurOSC[j],
                                   " and ", 
                                   UFVar,
                                   # " LIMIT 10000", # apenas para testes
                                   ";"))
      
      # Salva os dados extraídos no diretório de download
      saveRDS(rawData, paste0(downloadDir, NameFile))
      
      # Vou fazer as buscas dormirem um pouco para não ser confundido
      # com um ataque ao servidor
      Sys.sleep(sample(1:3, 1))
      
      rm(rawData, NameFile, UFVar)
    }
    rm(j)
  }
  rm(i)
}
rm(h)

rm(ArquivosAno, UFs, NatJurOSC)
rm(Tables, downloadDir)
dbDisconnect(connec)
rm(connec)

# Fim ####

# Sobras:

# Examina uma amostra da base:
# names2020 <- names(teste)
# for(i in seq_len(nrow(ArquivosAno))) {
  teste <- dbGetQuery(connec,
                      paste0("SELECT * FROM ", ArquivosAno$Arquivo[i],
                             " LIMIT 10000",
                             ";"))
#   
#   # print(all(c("uf_ipea", "natureza_juridica", "ano") %in% names(teste)))
#   print(all(names2020 %in% names(teste)))
#   Sys.sleep(sample(1:3, 1))
# }
# 
# rm(teste, names2020)
