# Função para economizar linhas ao PostgressSQL

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-06-28

# Setup ####

library(DBI)
library(RODBC)
library(RPostgres)
library(assertthat)
library(jsonlite)

# Debug: ####
# KeyFile <- "keys/localhost_key.json"
# Con_options <- "-c search_path=vinculos_v6"

# Function ####

postCon <- function(KeyFile, Con_options = NULL) {
  
  assert_that(file.exists(KeyFile), 
              msg = paste("Arquivo de chaves", 
                          KeyFile, "não encontrado."))
  
  assert_that(str_sub(KeyFile, -4, -1) == "json", 
              msg = "O arquivo de chaves não é json")
  
  # Carrega arquivo de chaves
  keys <- jsonlite::read_json(KeyFile)

  # Elementos necessários para o arquivo de chave
  KeyRequiredNames <- c("username", "password", "dbname", "host", "port")
  
  assert_that(all(KeyRequiredNames %in% names(keys)), 
              msg = paste("Seguintes elementos faltando no arquivo de chaves:",
                          paste(KeyRequiredNames[!(KeyRequiredNames %in% names(keys))], 
                                collapse = ", ")))
  rm(KeyRequiredNames)
  
  # Verifica se pode conectar
  TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                              dbname = keys$dbname,
                              host = keys$host,
                              port = keys$port,
                              user = keys$username, 
                              password = keys$password,
                              options = Con_options)
  
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
                      options=Con_options)
  
  # Verifica a coneção com a base
  assert_that(dbIsValid(connec))
  
  return(connec)
  rm(keys, TestConexao)
}
# rm(KeyFile, Con_options, connec)

# Fim ####