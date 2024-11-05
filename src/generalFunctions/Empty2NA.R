# Função para transformar variáveis vazias ("") em NA

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-11-05

# Setup ####

library(assertthat)

# Debug:
# x_df <- readRDS("C:/Users/P22433438861/Desktop/MOSCProj/mapa-osc-data-import/backup_files/2024_01/output_files/tb_localizacao.RDS")

# Função ####

Empty2NA <- function(x_df, Vars = "All") {
  
  assert_that(is.character(Vars))
  assert_that("data.frame" %in% class(x_df))
  
  if(Vars == "All") {
    
    for (j in names(x_df) ) {
      # j <- names(x_df)[7]
      # print(j)
      if(!"POSIXt" %in% class(x_df[[j]]) ) { # O método abaixo não funciona com datas
        x_df[[j]][ x_df[[j]] == "" ] <- NA
      }
    }
    
  } else {
    
    assert_that( all( Vars %in% names(x_df) ) )
    
    for (j in Vars ) {
      # j <- names(x_df)[7]
      # print(j)
      if(!"POSIXt" %in% class(x_df[[j]]) ) { # O método abaixo não funciona com datas
        x_df[[j]][ x_df[[j]] == "" ] <- NA
      }
    }
  }
  
  return(x_df)
  
}

# Fim ####