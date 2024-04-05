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
# Table_NameAntigo <- names(ArquivosAtualizacao)[i] 
# verbose = TRUE
# samples = TRUE
# # rm(DadosNovos, Chave, Conexao, Table_NameAntigo)
# ls()


AtualizaDados <- function(Conexao, DadosNovos, Chave, Table_NameAntigo, 
                          verbose = FALSE, samples = TRUE, deleterows = FALSE, 
                          GeoVar = NULL) {
  
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
  
  # Se tiver variável geográfica, verificar se a função do postgis está funcionando
  if(!is.null(GeoVar)) {
    TesteFuncGeo <- try(dbGetQuery(Conexao, "SELECT public.ST_MakePoint(-50.3482090039996,-20.7619611619996);"))
    assert_that(!is.error(TesteFuncGeo), 
                msg = "Função 'public.ST_MakePoint' não encontrada")
    rm(TesteFuncGeo)
    }
  
  ## Verifica se o nome da tabela a ser atualizada inserida está correto:
  assert_that(is.character(Table_NameAntigo) & length(Table_NameAntigo) == 1)
  Tables <- dbListTables(Conexao)
  assert_that(Table_NameAntigo %in% Tables, 
              msg = "Tabela de atualização não encontrada no banco de dados")
  
  # Baixa dados do banco a se atualizar
  message("Baixando dados antigos...")
  DadosAntigos <- dbGetQuery(Conexao, 
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
    
    # Se for variável geográfica, o tratamento vai ser feito baixo
    if(!(i %in% GeoVar)) {
      assert_that(all(class(DadosAntigos[[i]]) == class(DadosNovos[[i]])), 
                  msg = paste("A coluna", i, "não é do mesmo tipo nos dois", 
                              "bancos de dados")) 
      # class(DadosAntigos[[i]]) 
      # class(DadosNovos[[i]])
    }
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
  
  if(deleterows) {
    
    message("Remove linhas excluídas da tabela")
    
    ## Cria uma cópia dos dados antigos só com os IDs e 
    ## uma variável de se deve ou não deletar
    DeleteData <- tibble(.rows = nrow(DadosAntigos)) 
    DeleteData[[Chave]] <- DadosAntigos[[Chave]]
    DeleteData[["deletar"]] <- !(DadosAntigos[[Chave]] %in% DadosNovos[[Chave]])
    
    if(sum(DeleteData[["deletar"]]) > 0) {
      
      if(samples) {
        # Amostra de linhas deletadas
        SampleDel <- sample(DeleteData[[Chave]][DeleteData$deletar], min(20, sum(DeleteData$deletar)))
        message("Amostra de linhas deletadas: ", Chave, " == ", 
                paste0(SampleDel, collapse = ", "))
        rm(SampleDel)
      }
      
      ## Faz upload da tabela com as linhas a se deletar
      if(dbExistsTable(Conexao, "deletedata")) dbRemoveTable(Conexao, "deletedata")
      dbWriteTable(Conexao, "deletedata", DeleteData)
      
      # Faz umm join desta tabela com os dados antigos
      
      # Deleta a coluna, se ela existir:
      query_DropCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                              " DROP COLUMN IF EXISTS deletar;")
      
      if("deletar" %in% names(DadosAntigos)) {
        dbExecute(Conexao, query_DropCol)
      }
      
      # Cria coluna na tabela de dados antigos
      query_AddCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                             " ADD COLUMN deletar boolean;")
      
      dbExecute(Conexao, query_AddCol)
      
      # Insere a coluna com as linhas para deletar
      query_JoinDelete <- paste0("UPDATE ", Table_NameAntigo, "\n",
                                 " SET deletar = deletedata.deletar", "\n",
                                 " FROM deletedata", "\n",
                                 " WHERE ", Table_NameAntigo, ".", Chave,
                                 " = deletedata.", Chave,
                                 ";")
      # cat(query_JoinDelete)
      
      # query_JoinDelete
      dbExecute(Conexao, query_JoinDelete)
      
      # Deleta as linhas com base na nova coluna
      query_DeleteRows <- paste0("DELETE FROM ", Table_NameAntigo, 
                                 " WHERE deletar;")
      # query_DeleteRows
      LinhasDeletadas <- dbExecute(Conexao, query_DeleteRows)
      
      message(LinhasDeletadas, " linhas deletadas da tabela")
      
      # Deleta a coluna criada
      dbExecute(Conexao, query_DropCol)
      
      # Remove a tabela deletedata
      if(dbExistsTable(Conexao, "deletedata")) dbRemoveTable(Conexao, "deletedata")
      
      rm(query_JoinDelete, query_DropCol, query_DeleteRows, query_AddCol, 
         LinhasDeletadas)
      
    } else {
      message("Nenhuma linha deletada do banco")
    }
    rm(DeleteData)
    
  }
  
  
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Insere linhas novas que não estavam no banco antigo ####
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  message("Inserindo linhas novas no banco")
  
  # Atualiza a tabela, para evitar problemas:
  message("Atualizando novamente os dados")
  DadosAntigos <- dbGetQuery(Conexao, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))
  
  # Extrai as linhas que não estão no banco antigo
  DadosNovos[["AddRows"]] <- !(DadosNovos[[Chave]] %in% DadosAntigos[[Chave]])
  
  if(sum(DadosNovos[["AddRows"]]) > 0) {
    
    if(samples) {
      # Amostra de linhas inseridas
      SampleAdd <- sample(DadosNovos[[Chave]][DadosNovos$AddRows], 20)
      message("Amostra de linhas inseridas: ", Chave, " == ", 
              paste0(SampleAdd, collapse = ", "))
      rm(SampleAdd)
    }
    
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
    rm(j)
    
    # Insere colunas faltantes
    for (j in MissingCols) {
      # print(j) 
      AddData[[j]] <- NA
    }
    rm(j)
    # names(AddData)
    # names(AddData) %in% names(DadosAntigos)
    # names(DadosAntigos) %in% names(AddData)
    # names(AddData)[names(AddData) %in% names(DadosAntigos)]
    
    ## Insere linhas:
    AddedRows <- dbAppendTable(Conexao, Table_NameAntigo, AddData)
    
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
  message("Atualizando novamente os dados da tabela do BD")
  DadosAntigos <- dbGetQuery(Conexao, 
                             paste0("SELECT * FROM ",
                                    Table_NameAntigo,
                                    # " LIMIT 500", 
                                    ";"))

  # Atualiza coluna a coluna, usando a estratégia da tabela
  # intermediária.
  for (col in Att_Cols) {
    # col <- Att_Cols[11]
    # print(col)
    
    message("Atualizando coluna ", col)
    
    # Variável que será atualizada com o nome padronizado
    DadosCheck <- DadosNovos[, c(Chave, col)]
    names(DadosCheck) <- c(Chave, "Dado")
    
    # Tabela temporária para inserir os dados atualizados
    DadosUpdate <- tibble(.rows = nrow(DadosAntigos))
    DadosUpdate[[Chave]] <- DadosAntigos[[Chave]]
    DadosUpdate[["Dado_Old"]] <- DadosAntigos[[col]]
    
    # Estou aqui !!!! ####
    # Tratamento especial das variáveis geográficas
    if(col %in% GeoVar) {
      
      # Problema de variáveis com MAIÙSCULA
      DadosCheck[["geo_dado"]] <- DadosCheck[["Dado"]]
      
      ## Tabela temporária para formatar dados geográficos
      if(dbExistsTable(Conexao, "geo_temp")) dbRemoveTable(Conexao, "geo_temp")
      
      dbWriteTable(Conexao, "geo_temp", 
                   select(DadosCheck, 
                          all_of(Chave), geo_dado))
      
      x <- dbGetQuery(Conexao, "SELECT public.ST_GeomFromText(geo_dado, 4674) FROM geo_temp;")
      DadosCheck[["Dado"]] <- x$st_geomfromtext
      
      if(dbExistsTable(Conexao, "geo_temp")) dbRemoveTable(Conexao, "geo_temp")
      
      DadosCheck[["geo_dado"]] <- NULL
      rm(x)
    }  

    DadosUpdate <- DadosUpdate %>% 
      left_join(DadosCheck, by = Chave) %>% 
      mutate(Atualiza = case_when(
        ## O dado novo não por der NA
        is.na(Dado) ~ FALSE, 
        ## Se o dado novo não for NA e o antigo sim
        !is.na(Dado) & is.na(Dado_Old) ~ TRUE,
        # Se os dados forem iguais, não precisa atualizar
        Dado == Dado_Old ~ FALSE,
        # Vou deixar isso aqui para os outros casos:
        TRUE ~ TRUE), 
        temp_var = ifelse(Atualiza, Dado, Dado_Old)) %>% 
      # select(all_of(Chave), temp_var) %>% 
      select(everything())
    
    if(sum(DadosUpdate$Atualiza) > 0) {
      
      message(sum(DadosUpdate$Atualiza), 
              " linhas serão atualizadas")
      
      # Corrige a formatação do dado de data
      if(is.Date(DadosUpdate$Dado_Old)) {
        DadosUpdate$temp_var <- as_date(DadosUpdate$temp_var)
      }
      
      # table(DadosUpdate$temp_var, useNA = "always")
      
      if(samples) {
        # Amostra de linhas atualizadas
        LinhasAtualizadas <- DadosUpdate %>% 
          dplyr::filter(Atualiza)
        
        SampleUpdate <- LinhasAtualizadas %>% 
          slice(sample(seq_len(nrow(LinhasAtualizadas)), 
                       min(20, nrow(LinhasAtualizadas)))) %>% 
          select(all_of(Chave), Dado, Dado_Old)
        
        message("Amostra de linhas atualizadas da coluna ", col, ": ")
        print(SampleUpdate)
        rm(SampleUpdate, LinhasAtualizadas)
      }
      
      ## Faz upload da tabela temporária com as linhas de atualização
      if(dbExistsTable(Conexao, "update_temp")) dbRemoveTable(Conexao, "update_temp")
      
      dbWriteTable(Conexao, "update_temp", 
                   select(DadosUpdate, 
                          all_of(Chave), temp_var))
      # teste <- dbGetQuery(Conexao, "SELECT * FROM update_temp")
      # View(teste)
      # table(teste$temp_var, useNA = "always")
      
      # Faz umm join desta tabela com os dados antigos
      
      # Deleta a coluna, se ela existir:
      query_DropCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                              " DROP COLUMN IF EXISTS temp_var;")
      if("temp_var" %in% names(DadosAntigos)) {
        dbExecute(Conexao, query_DropCol)
      }
      
      # Pega o tipo de variável a ser criado:
      QueryGetInformation <- paste0("SELECT *", "\n",
                                    "FROM information_schema.columns", "\n",
                                    "WHERE table_name = 'update_temp';")
      # cat(QueryGetInformation)
      ColTypes <- dbGetQuery(Conexao, QueryGetInformation)
      VarType <- ColTypes$data_type[ColTypes$column_name == "temp_var"]
      
      # Cria coluna na tabela de dados antigos
      query_AddCol <- paste0("ALTER TABLE ", Table_NameAntigo, 
                             " ADD COLUMN temp_var ",  
                             toupper(VarType), 
                             ";")
      
      dbExecute(Conexao, query_AddCol)
      
      rm(QueryGetInformation, ColTypes, VarType, query_AddCol)
      
      # Insere a coluna com as linhas para deletar
      query_JoinUpdate <- paste0("UPDATE ", Table_NameAntigo, "\n",
                                 " SET temp_var = update_temp.temp_var", "\n",
                                 " FROM update_temp", "\n",
                                 " WHERE ", Table_NameAntigo, ".", Chave,
                                 " = update_temp.", Chave,
                                 ";")
      # cat(query_JoinUpdate)
      
      # Executa a inserção das colunas
      dbExecute(Conexao, query_JoinUpdate)
      
      # Determina a atualização da coluna com base no valor de temp_var
      query_UpdateCol <- paste0("UPDATE ", Table_NameAntigo, "\n",
                                " SET ", col, " = temp_var",
                                ";")
      # cat(query_UpdateCol)
      
      # query_DeleteRows
      LinhasAtualizadas <- dbExecute(Conexao, query_UpdateCol)
      
      message("Linhas atualizadas na coluna ", col)
      
      # Deleta a coluna criada
      dbExecute(Conexao, query_DropCol)
      
      # Remove a tabela deletedata
      if(dbExistsTable(Conexao, "update_temp")) {
        dbRemoveTable(Conexao, "update_temp")
      }
      
      # ls()
      rm(query_UpdateCol, query_DropCol, query_JoinUpdate, 
         LinhasAtualizadas)
      
    } else {
      message("Não há linhas na tabela '", col, 
              "' para serem atualizadas.")
    }
    rm(DadosCheck, DadosUpdate)
  }
  rm(col)
  
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Finaliza Atualização
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  rm(Att_Cols, Tables, DadosAntigos)
  # ls()
  
  message("Atualização Concluída!")
  message("Marcação do tempo: ", now())
  return(TRUE)
}

# Fim ####

