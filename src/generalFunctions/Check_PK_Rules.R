# Função para garantir chaves primárias consistentes

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-11-05

# Setup ####

library(assertthat)

# Debug:
# pk_vector <- tb_osc$id_osc

# Função ####

Check_PK_Rules <- function(pk_vector) {
  
  # Garante chaves primárias únicas
  assert_that(length(pk_vector) == length(unique(pk_vector)),
              msg = "Chaves primárias não únicas.")
  
  # Garante chaves primárias não nulas
  assert_that( sum( is.na(pk_vector) ) == 0 , 
              msg = "Criação de nulos em chaves primárias")
  
  return(TRUE)
  
}

# Fim ####