# Função para atualizar o 

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-02-18

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)

# Debug:
# DadosNovos <- tb_osc_New
# DadosNovos <- slice(tb_osc_New, 1:500)
# DadosNovos[["cd_identificador_osc"]] <- as.numeric(DadosNovos[["cd_identificador_osc"]])
# Chave <- "id_osc"
# Conexao <- connec
# Table_NameAntigo <- "tb_osc_bckp"
# verbose = TRUE
# # rm(DadosNovos, Chave, Conexao, Table_NameAntigo)
# ls()


AtualizaDados <- function(Conexao, DadosNovos, Chave, Table_NameAntigo, 
                          verbose = FALSE) {
  
  message("Marcação do tempo: ", now())
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Preparação da função ####
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
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
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Remove linhas que  não estão mais em DadosNovos ####
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  message("Remove linhas excluídas da tabela")
  
  ## Cria uma cópia dos dados antigos só com os IDs e 
  ## uma variável de se deve ou não deletar
  DeleteData <- tibble(.rows = nrow(DadosAntigos)) 
  DeleteData[[Chave]] <- DadosAntigos[[Chave]]
  DeleteData[["deletar"]] <- !(DadosAntigos[[Chave]] %in% DadosNovos[[Chave]])
  # sum(DeleteData[["deletar"]])
  
  ## Faz upload da tabela com as linhas a se deletar
  if(dbExistsTable(connec, "deletedata")) {
    dbRemoveTable(connec, "deletedata")
  }
  dbWriteTable(connec, "deletedata", DeleteData)
  
  # Faz umm join desta tabela com os dados antigos
  
  # Deleta a coluna, se ela existir:
  query_DropCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                          " DROP COLUMN IF EXISTS deletar;")
  if("deletar" %in% names(DadosAntigos)) {
    dbExecute(connec, query_DropCol)
  }
  
  # Cria coluna na tabela de dados antigos
  query_AddCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                         " ADD COLUMN deletar boolean;")
  
  dbExecute(connec, query_AddCol)
  
  # Insere a coluna com as linhas para deletar
  query_JoinDelete <- paste0("UPDATE ", Table_NameAntigo, "\n",
                             " SET deletar = deletedata.deletar", "\n",
                             " FROM deletedata", "\n",
                             " WHERE ", Table_NameAntigo, ".", Chave,
                             " = deletedata.", Chave,
                             ";")
  # cat(query_JoinDelete)
  
  # query_JoinDelete
  dbExecute(connec, query_JoinDelete)
  
  # Deleta as linhas com base na nova coluna
  query_DeleteRows <- paste0("DELETE FROM ", Table_NameAntigo, 
                             " WHERE deletar;")
  # query_DeleteRows
  LinhasDeletadas <- dbExecute(connec, query_DeleteRows)
  
  message(LinhasDeletadas, " linhas deletadas da tabela")
  
  # Deleta a coluna criada
  dbExecute(connec, query_DropCol)
  
  # Remove a tabela deletedata
  if(dbExistsTable(connec, "deletedata")) {
    dbRemoveTable(connec, "deletedata")
  }
  
  rm(query_JoinDelete, query_DropCol, query_DeleteRows, query_AddCol)
  rm(DeleteData, LinhasDeletadas)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Insere linhas novas que não estavam no banco antigo ####
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  message("Inserindo linhas novas no banco")
  
  # Atualiza a tabela, para evitar problemas:
  message("Atualizando novamente os dados")
  DadosAntigos <- dbGetQuery(connec, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  # Extrai as linhas que não estão no banco antigo
  DadosNovos[["AddRows"]] <- !(DadosNovos[[Chave]] %in% DadosAntigos[[Chave]])
  
  if(sum(DadosNovos[["AddRows"]]) > 0) {
    
    ## Avisa das colunas que não serão atualizadas:
    MissingCols <- character(0)
    if(!all(names(DadosNovos) %in% names(DadosAntigos))) {
      MissingCols <- names(DadosAntigos)[!names(DadosAntigos) %in% names(DadosNovos)]
    } 
    
    # Linhas novas a ser inseridas
    AddData <- DadosNovos[DadosNovos[["AddRows"]], ]
    
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
    AddedRows <- dbAppendTable(connec, Table_NameAntigo, AddData)
    
    message(AddedRows, " linhas novas inseridas na tabela")
    
    rm(AddData, MissingCols, AddedRows)
  } else {
    message("Nenhuma linha nova inserida no banco")
  }
  
  
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Atualiza linhas existentes no banco ####
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Atualiza o banco em blocos de 1000 linhas:
  message("Iniciando atualização no banco: ")
  
  # Atualiza a tabela, para evitar problemas:
  message("Atualizando novamente os dados")
  DadosAntigos <- dbGetQuery(connec, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  ## Em primeiro lutar, não precisamos nos preocupar com linhas do banco 
  ## novo que foram recem inseridas. Elas não precisa ser atualizadas
  DadosUpdate <- DadosNovos[!DadosNovos[["AddRows"]], ]
  
  message(nrow(DadosUpdate), " linhas serão atualizadas no banco.")
  
  # Divide o banco em chucks de 1000 linhas
  ids <- DadosUpdate[[Chave]]
  split_OSC <- split(ids, ceiling(seq_along(ids)/1000))
  # length(split_OSC)
  rm(ids)
  
  for (i in seq_along(split_OSC)) {
    # i <- 19
    
    # Com verbose, mostrar a cada linha, sem, a cada 5000 linhas
    if(verbose) {
      message("Verificando linha ", ifelse(i == 1, 1, i*1000+1), 
              " de ", nrow(DadosUpdate))
    } else {
      # Mensagem a cada 5000 linhas verificadas
      if(i == 1 | i %% 5 == 0) {
        message("Verificando linha ", ifelse(i==1, 1, i*1000+1), 
                " de ", nrow(DadosUpdate))
      }
    }
    
    # Seleciona apenas um chuck de 1000 atualização para:
    DadosUpdate_i <- DadosUpdate[DadosUpdate[[Chave]] %in% split_OSC[[i]], ]
    DadosAntigos_i <- DadosAntigos[DadosAntigos[[Chave]] %in% DadosUpdate_i[[Chave]], ]
    
    # Evita problemas de formatação e notação científica
    DadosUpdate_i[[Chave]] <- as.character(format(DadosUpdate_i[[Chave]], 
                                                  scientific = FALSE))
    DadosAntigos_i[[Chave]] <- as.character(format(DadosAntigos_i[[Chave]], 
                                                   scientific = FALSE))
    
    # SQL que inicia a atualização
    SQL_Cabeca <- paste("UPDATE",  Table_NameAntigo, "\n\n SET ")
    
    # Chaves das linhas que serão atualizadas:
    KeysUpdated <- character(0)
    
    # Atualiza por coluna:
    SQL_Coluna <- ""
    for (j in Att_Cols) {
      # j <- Att_Cols[1]
      # print(j)
      
      # Seleciona apenas a coluna que será atualizada e a pk
      DadosAntigos_j <- DadosAntigos_i[, c(Chave, j)] %>% 
        # Padroniza os nomes para facilitar o dplyr
        magrittr::set_names(c("id", "Dado_Old"))
      
      # table(DadosAntigos_j$tx_apelido_osc_Old, useNA = "always")
      # names(Alteracao)
      
      # Seleciona apenas a coluna que será atualizada e a pk
      Alteracao <- DadosUpdate_i[, c(Chave, j)] %>% 
        # Padroniza os nomes para facilitar o dplyr
        magrittr::set_names(c("id", "Dado")) %>% 
        # Insere os dados antigos
        left_join(DadosAntigos_j, by = "id") %>% 
        # Condições para atualização:
        mutate(Atualiza = case_when(
          ## O dado novo não por der NA
          is.na(Dado) ~ FALSE, 
          ## Se o dado novo não for NA e o antigo sim
          is.na(Dado_Old) & !is.na(Dado) ~ TRUE,
          # Se os dados forem iguais, não precisa atualizar
          Dado_Old == Dado_Old ~ FALSE,
          # Vou deixar isso aqui para os outros casos:
          TRUE ~ TRUE), 
          # Conserta problema das aspas simples (') no Postree (coloca '')
          Dado = ifelse(str_detect(Dado, "'"), 
                        str_replace_all(Dado, "'", fixed("''")), 
                        Dado) ) %>% 
        # Mantem apenas linhas que serão atualizadas
        dplyr::filter(Atualiza)
      
      # Se não há linhas a se atualizar, pula
      if(nrow(Alteracao) > 0) {
        
        # String único da coluna a ser alterada:
        ColUpdate <- paste0("WHEN '", 
                            Alteracao[["id"]], 
                            "' THEN '",  
                            Alteracao[["Dado"]], 
                            "'\n", collapse = " ") %>% 
          str_remove("\n$")
        
        SQL_Coluna <- paste0(SQL_Coluna, j, " = CASE ", Chave,"\n ",
                             ColUpdate, 
                             "\n END, \n\n")
        
        KeysUpdated <- c(KeysUpdated, Alteracao[["id"]])
        rm(ColUpdate)
      }
      rm(Alteracao, DadosAntigos_j)
      # cat(SQL_Coluna)
    }
    rm(j, DadosUpdate_i, DadosAntigos_i)
    
    # Retira a última vírgula
    SQL_Coluna <- str_replace(SQL_Coluna, ", \n\n$", "\n\n")
    # cat(SQL_Coluna)
    
    # Atualiza se tiver algo a se atualizar
    if(length(KeysUpdated) > 0) {
      # Finalização da query:
      SQL_Comand <- paste(SQL_Cabeca, SQL_Coluna,
                          paste0("WHERE ", Chave, " IN (", 
                                 paste0("'", 
                                        unique(KeysUpdated), 
                                        "'", 
                                        collapse = ", "), 
                                 ");"))
      # cat(SQL_Comand)
      
      # Executa a Query:
      update_db <- dbSendQuery(connec, SQL_Comand)
      
      # Limpa resultado
      dbClearResult(update_db)
      rm(SQL_Comand, update_db)
    }
    rm(SQL_Cabeca, SQL_Coluna, KeysUpdated)
    # ls()
  }
  rm(i, DadosUpdate, split_OSC)
  
  # Finaliza Atualização
  
  rm(Att_Cols, Tables, DadosAntigos)
  # ls()
  
  message("Atualização Concluída!")
  message("Marcação do tempo: ", now())
}

# Fim ####