
# Função para corrigir nomes duplicados em data frames

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2026-02-12

fix_duplicate_names <- function(df, alert = TRUE) {
  # df <- tb_JoinOSC
  
  # Verifica se há nomes duplicados:
  names_df <- names(df)
  table_names <- table(names_df)
  
  # Caso não houver nomes duplicados:
  if( max(table_names) == 1 ) {
    
    if(alert) message("Não foi encontrado nomes duplicados")
    
    # Rotina para remover nomes duplicados:
  } else {
    
    nomes_duplicados <- names(table_names)[which(table_names > 1)]
    
    if(alert) message("Encontrados os seguintes nomes duplicados: ", 
                      paste(nomes_duplicados, collapse = ", "))
    
    for (i in seq_along(nomes_duplicados) ) {
      # i <- 1
      
      posicoes_duplicados <- which(names(df) %in% nomes_duplicados[i])
      
      for (j in seq_along(posicoes_duplicados) ) {
        # j <- 2
        
        # Na primeira ocorrência da duplicata, mantem o nome, nas seguintes
        # coloca um integral
        if(j > 1) {
          names(df)[ posicoes_duplicados[j] ] <- paste0(nomes_duplicados[i], j)
        }
      }
      rm(posicoes_duplicados)
      
    }
    rm(nomes_duplicados)
    
  }
  
  return(df)
  rm(names_df, table_names)
}


# Fim ####