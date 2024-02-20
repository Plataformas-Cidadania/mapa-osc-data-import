
# Visualização de dados do Mapa Postgree

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2023-08-18

# install.packages('RPostgres')
library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Coneção à Base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Verifica se pode condenar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
             dbname = "mapa_osc",
             host = "localhost",
             port = "5432",
             user = "postgres", 
             password = "I&E$hx*UxY4Akzph", 
             options="-c search_path=osc")

assert_that(TestConexao)

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = "mapa_osc",
                    host = "localhost",
                    port = "5432",
                    user = "postgres", 
                    password = "I&E$hx*UxY4Akzph", 
                    options="-c search_path=osc")

# Verifica a coneção com a base
# connec


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Leitura de Dados ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tb_osc_New <- readRDS("backup_files/2023_01/output_files/tb_osc.RDS")
# tb_dados_gerais_New <- readRDS("backup_files/2023_01/output_files/tb_dados_gerais.RDS")
# tb_area_atuacao_New <- readRDS("backup_files/2023_01/output_files/tb_area_atuacao.RDS")
# tb_contato_New <- readRDS("backup_files/2023_01/output_files/tb_contato.RDS")

# table(tb_osc$ft_apelido_osc)
# table(tb_osc$ft_osc_ativa)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Criando uma tabela de backup ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if(dbExistsTable(connec, "tb_osc_bckp")){
  dbRemoveTable(connec, "tb_osc_bckp")
}
dbWriteTable(connec, "tb_osc_bckp", tb_osc)
dbExistsTable(connec, "tb_osc_bckp")

# tb_osc_bckp <- dbGetQuery(connec, paste0("SELECT * FROM tb_osc_bckp",
#                                     # " LIMIT 1000",
#                                     ";"))

names(tb_osc_bckp)
View(tb_osc_bckp)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Funções de atualização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Debug:
# DadosNovos <- tb_osc_New
DadosNovos <- slice(tb_osc_New, 34000:35000)
DadosNovos[["cd_identificador_osc"]] <- as.numeric(DadosNovos[["cd_identificador_osc"]])
# Chave <- "id_osc"
# Conexao <- connec
# Table_NameAntigo <- "tb_osc_bckp"
# verbose = TRUE
# rm(DadosNovos, Chave, Conexao, Table_NameAntigo)
# ls()


AtualizaDados <- function(Conexao, DadosNovos, Chave, Table_NameAntigo, 
                          verbose = FALSE) {

  # Abaixo está uma série de teste para evitar inconsistências e erros no
  # código.
  
  # Vamos começar verificando a conexão e a existência da tabela a se atualizar
  
  ## Checa se a conexão está funcionando
  assert_that(class(Conexao)[1] == "PqConnection")
  assert_that(DBI::dbIsValid(Conexao))
  
  ## Verifica se o nome da tabela a ser atualizada inserida está correto:
  assert_that(is.character(Table_NameAntigo) & length(Table_NameAntigo) == 1)
  Tables <- dbListTables(connec)
  assert_that(Table_NameAntigo %in% Tables)
  
  # Baixa dados do banco a se atualizar
  message("Baixando dados antigos...")
  DadosAntigos <- dbGetQuery(connec, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  # Vamos verificar agora se os dados a se atualizar e os antigos
  # estão adequados:
  
  ## Checa se a chave primária está em ambos os bancos de dados
  assert_that(Chave %in% names(DadosNovos), 
              msg = "A chave não está na tabela de dados novos")
  
  assert_that(Chave %in% names(DadosAntigos), 
              msg = "A chave não está na tabela de dados antigos")
  
  ## Colunas a se atualizar estão no banco novo:
  Att_Cols <- names(DadosNovos)[names(DadosNovos) %in% names(DadosAntigos)]
  Att_Cols <- Att_Cols[Att_Cols != Chave]
  
  assert_that(length(Att_Cols) > 0, 
              msg = "não há campos a serem atualizados")

  ## Verifica se as colunas novas e antigas são do mesmo tipo:
  
  ### Verifica coluna por coluna
  for (i in Att_Cols) {
    # i <- Att_Cols[1]
    assert_that(all(class(DadosAntigos[[i]]) == class(DadosNovos[[i]])), 
                msg = paste("A coluna", i, "não é do mesmo tipo nos dois", 
                            "bancos de dados")) 
    # class(DadosAntigos[[i]]) 
    # class(DadosNovos[[i]])
  }
  rm(i)

  ## Avisa das colunas que não serão atualizadas:
  if(!all(names(DadosNovos) %in% names(DadosAntigos))) {
    message(paste0("Os campos '", 
                   paste0(names(DadosAntigos)[!names(DadosNovos) %in% names(DadosAntigos)],
                          collapse = "', '"),
                   "' não estão na tabela de atualização."))  
  }  
  
  # Atualiza o banco linha por linha:
  
  message("Iniciando atualização no banco: ", "1 de ", nrow(DadosNovos))
  
  
  for (i in seq_len(nrow(DadosNovos))) {
    # i <- 862 
    
    # Com verbose, mostrar a cada linha, sem, a cada 5000 linhas
    if(verbose) {
      message("Verificando linha ", i, " de ", nrow(DadosNovos))
    } else {
      # Mensagem a cada 1000 linhas verificadas
      if(i %% 1000 == 0) {
        message("Verificando linha ", i, " de ", nrow(DadosNovos))
      }
    }
    
    # Verifica se a linha do dado novo está no antigo.
    # Se não está, pula para o próximo
    if(DadosNovos[[Chave]][i] %in% DadosAntigos[[Chave]]) {
      
      if(verbose) message("Linha Existente")
      
      # Linha no banco antigo onde está a referência do novo
      # (para optimizar a busca)
      i_2 <- which(DadosAntigos[[Chave]] == DadosNovos[[Chave]][i])
      
      # DadosAntigos[[Chave]][[i_2]]
      # DadosNovos[[Chave]][i]
      
      # Contador de atualizações necessárias (se for zero, nem atualiza)
      Att_Count <- 0
      
      # Começo da query:
      Query_tx <- paste0("UPDATE ", Table_NameAntigo, " SET ")
             
      # Verificar em cada coluna o que precisa de atualização
      for (j in Att_Cols) {
        # j <- Att_Cols[1]
        # print(j)
        
        # Condições para atualização:
        
        ## O dado novo não por der NA
        DummyAtualiza <- !is.na(DadosNovos[[j]][[i]])
        
        ## O dado antigo tem que ser diferente do novo ou ser NA
        DummyAtualiza <- DummyAtualiza &
          ( DadosNovos[[j]][[i]] != DadosAntigos[[j]][[i_2]] ||
             is.na(DadosAntigos[[j]][[i_2]]) )
        
        # Se o dado for diferente na coluna, atualizar
        if(DummyAtualiza) {
          
          ReplaceValue <- DadosNovos[[j]][[i]]
          
          # Conserta problema das aspas simples (') no Postree (coloca '')
          if(str_detect(ReplaceValue, "'")) {
            ReplaceValue <- str_replace_all(ReplaceValue, "'", fixed("''"))
          }
          
          # Adiciona o campo a ser atualizado na query
          Query_tx <- paste0(Query_tx, 
                             j, " = '", ReplaceValue, 
                             "', ")
          
          # Atualiza contador de atualizações
          Att_Count <- Att_Count + 1
          rm(ReplaceValue)
        }
      }
      rm(j, DummyAtualiza)
      
      # Se teve alguma atualização, atualiza!
      if(Att_Count > 0) {
        
        # Remove última vírgula
        Query_tx <- str_replace(Query_tx, ", $", " ")
        
        # finaliza a query:
        Query_tx <- paste0(Query_tx, 
                           "WHERE ", Chave, " = ", DadosNovos[[Chave]][i], 
                           " ;")
        
        if(verbose) message(Query_tx)
        
        # Executa a Query:
        update_db <- dbSendQuery(Conexao, 
                                 Query_tx)
        
        # Limpa resultado
        dbClearResult(update_db)
        rm(update_db)
      }
      rm(Att_Count, Query_tx, i_2)
    } else {
    if(verbose) message("linha não econtrada")
    }
  }
  rm(i, Att_Cols, Tables, DadosAntigos)
}



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Função que insere dados novos e deleta antigos ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Debug:
# DadosNovos <- tb_osc_New
DadosNovos <- tb_osc_New
DadosNovos[["cd_identificador_osc"]] <- as.numeric(DadosNovos[["cd_identificador_osc"]])
# Chave <- "id_osc"
# Conexao <- connec
# Table_NameAntigo <- "tb_osc_bckp"
# verbose = TRUE
# rm(DadosNovos, Chave, Conexao, Table_NameAntigo)

# Estou aqui!!! ####

InsereDados <- function(Conexao, DadosNovos, Chave, Table_NameAntigo, 
                          verbose = FALSE) {
  
  # Abaixo está uma série de teste para evitar inconsistências e erros no
  # código.
  
  # Vamos começar verificando a conexão e a existência da tabela a se atualizar
  
  ## Checa se a conexão está funcionando
  assert_that(class(Conexao)[1] == "PqConnection")
  assert_that(DBI::dbIsValid(Conexao))
  
  ## Verifica se o nome da tabela a ser atualizada inserida está correto:
  assert_that(is.character(Table_NameAntigo) & length(Table_NameAntigo) == 1)
  Tables <- dbListTables(connec)
  assert_that(Table_NameAntigo %in% Tables)
  
  # Baixa dados do banco a se inserir
  message("Baixando dados antigos...")
  DadosAntigos <- dbGetQuery(connec, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  # Vamos verificar agora se os dados a se inserir e os antigos
  # estão adequados:
  
  ## Checa se a chave primária está em ambos os bancos de dados
  assert_that(Chave %in% names(DadosNovos), 
              msg = "A chave não está na tabela de dados novos")
  
  assert_that(Chave %in% names(DadosAntigos), 
              msg = "A chave não está na tabela de dados antigos")
  
  ## Colunas a se inserir estão no banco novo:
  Att_Cols <- names(DadosNovos)[names(DadosNovos) %in% names(DadosAntigos)]
  Att_Cols <- Att_Cols[Att_Cols != Chave]
  
  assert_that(length(Att_Cols) > 0, 
              msg = "não há campos a serem atualizados")
  
  ## Verifica se as colunas novas e antigas são do mesmo tipo:
  
  ### Verifica coluna por coluna
  for (i in Att_Cols) {
    # i <- Att_Cols[1]
    assert_that(all(class(DadosAntigos[[i]]) == class(DadosNovos[[i]])), 
                msg = paste("A coluna", i, "não é do mesmo tipo nos dois", 
                            "bancos de dados")) 
    # class(DadosAntigos[[i]]) 
    # class(DadosNovos[[i]])
  }
  rm(i)
  
  ## Avisa das colunas que não serão atualizadas:
  MissingCols <- character(0)
  if(!all(names(DadosNovos) %in% names(DadosAntigos))) {
    message(paste0("Os campos '", 
                   paste0(names(DadosAntigos)[!names(DadosNovos) %in% names(DadosAntigos)],
                          collapse = "', '"),
                   "' não estão na tabela de atualização."))
    
    MissingCols <- names(DadosAntigos)[!names(DadosAntigos) %in% names(DadosNovos)]
  }  
  
  
  # Extrai as linhas que não estão no banco antigo
  AddRows <- !(DadosNovos[[Chave]] %in% DadosAntigos[[Chave]])
  # sum(AddRows)
  AddData <- DadosNovos[AddRows, ]
  
  # Retira do banco novo colunas que não estão no antigo
  for (j in names(AddData)) {
    # print(j)
    if(!j %in% names(DadosAntigos)) {
      AddData[[j]] <- NULL
    }
  }
  
  # Insere colunas faltantes
  for (j in MissingCols) {
    # print(j) 
    AddData[[j]] <- NA
  }
  # names(AddData)
  # names(AddData) %in% names(DadosAntigos)
  # names(AddData)[names(AddData) %in% names(DadosAntigos)]
  
  ## Insere linhas:
  dbAppendTable(connec, Table_NameAntigo, AddData)
  
  # Agora vou remover dados que não estão mais em DadosNovos
  
  # Atualiza a tabela, para evitar problemas:
  DadosAntigos <- dbGetQuery(connec, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  ## Cria uma cópia dos dados antigos só com os IDs e se deve ou não deletar
  DeleteData <- tibble(.rows = nrow(DadosAntigos)) 
  DeleteData[[Chave]] <- DadosAntigos[[Chave]]
  DeleteData[["deletar"]] <- !(DadosAntigos[[Chave]] %in% DadosNovos[[Chave]])
  # sum(DeleteData[["deletar"]])
  
  ## Faz upload da tabela com as linhas a se deletar
  if(dbExistsTable(connec, "deletedata")) {
    dbRemoveTable(connec, "deletedata")
  }
  dbWriteTable(connec, "deletedata", DeleteData)

  # # insere chave primária na tabela:
  # dbExecute(connec, paste0("ALTER TABLE deletedata ", 
  #                          "ADD PRIMARY KEY (", 
  #                          Chave, ");"))
  
  # Faz umm join desta tabela com os dados antigos
  
  # Deleta a coluna, se ela existir:
  query_DropCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                          " DROP COLUMN IF EXISTS deletar;")
  
  dbExecute(connec, query_DropCol)
  
  # Cria coluna na tabela de dados antigos
  query_AddCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                         " ADD COLUMN deletar boolean;")
  
  dbExecute(connec, query_AddCol)

  # Insere a coluna com as linhas para deletar
  query_JoinDelete <- paste0("INSERT INTO ", Table_NameAntigo, "(deletar)",
                             " SELECT deletar ",
                             "FROM deletedata",
                             ";")
  # query_JoinDelete
  dbExecute(connec, query_JoinDelete)
  
  
  # Deleta as linhas com base na nova coluna
  query_DeleteRows <- paste0("DELETE FROM ", Table_NameAntigo, 
                             " WHERE deletar;")
  # query_DeleteRows
  dbExecute(connec, query_DeleteRows)
  
  # Deleta a coluna criada
  dbExecute(connec, query_DropCol)
  
  rm(query_JoinDelete, query_DropCol, query_DeleteRows, query_AddCol)
  rm(DeleteData)
  rm(Att_Cols, Tables, DadosAntigos)
  ls()
}

now()
AtualizaDados(Conexao = connec, 
              DadosNovos = DadosNovos, 
              Chave = "id_osc", 
              Table_NameAntigo = "tb_osc_bckp", 
              verbose = FALSE)
now()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finalização da rotina ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Desconecta da base
dbDisconnect(connec)

rm(connec, Tables)


# Fim ####