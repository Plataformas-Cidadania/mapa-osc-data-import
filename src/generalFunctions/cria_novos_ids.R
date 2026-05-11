# Função para uniformizar nomes de variáveis a partir de expressões regulares

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2026-03-06

# Setup ####

library(assertthat)
library(dplyr)

# Debug:
# dados <- data_cebas_as
# dados_old <- tb_certificado_old
# campos_chave <- c("id_osc", "cd_certificado", "dt_inicio_certificado",
#                   "cd_uf", "cd_municipio")
# id_campo <- "id_certificado"
# solve_double_pk = TRUE

# Função ####
cria_novos_ids <- function(dados, campos_chave, id_campo, 
                           dados_old = NULL,
                           solve_double_pk = TRUE ) {
  
  if( !is.null(dados_old) ) {
    
    # Cria uma chave de identificação da linha, com base em várias variáveis:
    for(j in seq_along(campos_chave) ) {
      if(j == 1) {
        chave_j <- dados[[ as.character( campos_chave[[1]] ) ]]
        chave_j_old <- dados_old[[ as.character( campos_chave[[1]] ) ]]
      } else {
        chave_j <- paste0( chave_j, "_", 
                           as.character( dados[[ campos_chave[[j]] ]] ) )
        chave_j_old <- paste0( chave_j_old, "_", 
                               as.character( dados_old[[ campos_chave[[j]] ]] ))
      }
    }
    rm(j)
    
    dados[["chave"]] <- chave_j
    dados_old[["chave"]] <- chave_j_old
    
    rm(chave_j, chave_j_old)
    
    dados[[id_campo]] <- NULL
    
    dados <- dados %>% 
      left_join(select(dados_old, all_of(id_campo), chave), 
                by = "chave") %>% 
      select(chave, all_of(id_campo), everything())
    
    message(sum(!is.na(dados$id_certificado)), " chaves antigas resgatadas")
    
    if(solve_double_pk){
      dados <- dados %>% 
        distinct(chave, .keep_all = TRUE)
    }
  }
  
  # Cria IDs novos
  if(sum(is.na(dados[[id_campo]] )) > 0) {
    # id máximo antigo
    if( !is.null(dados_old) ) {
      Max_OldID <- max(dados_old[[id_campo]], na.rm = TRUE)
    } else if( all(is.na(dados[[id_campo]]) ) ) {
      Max_OldID <- 0
    } else {
      Max_OldID <- max(dados[[id_campo]], na.rm = TRUE)
    } 
    
    # Novos IDs
    NewID <- seq(from = Max_OldID + 1, 
                 to = Max_OldID + sum(is.na( dados[[id_campo]] )), 
                 by = 1)
    
    # Adiciona novos IDs
    dados[[id_campo]][is.na( dados[[id_campo]] )] <- NewID
    
    assert_that(sum(is.na( dados[[id_campo]] )) == 0)
    
    message(length(NewID), " novos IDs criados.")
    
    rm(Max_OldID, NewID)
  }
     
     return(dados)
  
  rm(dados, dados_old, solve_double_pk, campos_chave, id_campo)
  ls()
}

# Fim ####