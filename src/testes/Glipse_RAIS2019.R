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


# Lista de Tabelas
Tables <- dbListTables(connec)

# Base em branco para listar os campos das tabelas
ListFields <- tibble::tibble(Table = character(0), 
                             Field = character(0), 
                             Description = character(0))

# Base em branco para agregar as informações de cada tabela.
TableData <- tibble(Table = character(0),
                    oid = character(0),
                    Nrows = character(0),
                    NCols = character(0), 
                    Description = character(0)) %>% 
  add_row(Table = Tables)

rm(Tables)

# Loop para uma amostra de 10.000 linhas de cada tabela, mais informações
# sobre as tabelas e seus campos (colunas ou variáveis).
for (i in seq_len(nrow(TableData))) {
  # i <- 1
  
  # Mensagem para verificar o loop
  message("Baixando tabela ", TableData$Table[i], " - ", i, 
          "/", length(TableData$Table))
  
  # Baixa uma amostra de 10.000 linhas.
  SampleData <- dbGetQuery(connec, paste0("SELECT * FROM ",
                                          TableData$Table[i],
                                          " LIMIT 10000",
                                          " ;"
                                          ))
  
  # Obtem o oid (id do objeto) da tabela.
  Table_oid <- dbGetQuery(connec, paste0("SELECT '", 
                                         TableData$Table[i], 
                                         "'::regclass::oid;"))[[1]]
  
  # Nomes das colunas
  TableComments <- character(ncol(SampleData))
  
  # Extrai os comentários dos campos
  for(j in seq_along(TableComments)) {
    # j <- 1
    TableComments[[j]] <- dbGetQuery(connec, paste0("SELECT pg_catalog.col_description(", 
                                                    Table_oid, ",", j ,")")) %>% 
      magrittr::extract2(1) %>% 
      as.character()
  }
  rm(j)
  
  # Salva a amostra de dados
  # fwrite(SampleData, paste0("data/temp/", 
  #                           TableData$Table[i], ".csv"), 
  #        sep = ";", dec = ",")
  
  saveRDS(SampleData, paste0("data/temp/", TableData$Table[i], ".RDS"))
  
  # Número de linhas da tabela
  TableData$Nrows[i] <- dbGetQuery(connec, 
                                   paste0("SELECT COUNT(*) FROM ", 
                                          TableData$Table[i])) %>% 
    magrittr::extract2(1) %>% 
    as.integer()
  
  # Número de colunas da tabela
  TableData$NCols[i] <- ncol(SampleData)
  
  # oid da tabela
  TableData$oid[i] <- Table_oid
  
  # Comentários dos campos da tabela
  TableData$Description[i] <- dbGetQuery(connec, paste0("SELECT obj_description(", Table_oid, 
                                                        ") FROM pg_class LIMIT 1")) %>% 
    magrittr::extract2(1) %>% 
    as.character()
  
  # Adiciona as informações dos campos da tabelas
  ListFields <- add_row(ListFields, 
                        Table = rep(TableData$Table[i], ncol(SampleData)), 
                        Field = names(SampleData), 
                        Description = TableComments)
  
  # Limpa memória
  rm(SampleData, Table_oid, TableComments)
}
rm(i)

# Salva metadados dos campos
fwrite(ListFields, "data/temp/ListFields.csv", 
       sep = ";", dec = ",")

# Salva metadados das tabelas
fwrite(TableData, "data/temp/TableData.csv", 
       sep = ";", dec = ",")

# Desconecta da base
dbDisconnect(connec)

rm(connec)
rm(ListFields, TableData)

# Fim
