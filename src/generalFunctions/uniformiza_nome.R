# Função para uniformizar nomes de variáveis a partir de expressões regulares

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2026-03-06

# Setup ####

library(assertthat)
library(stringi)
library(dplyr)

# Debug:
# x <- raw_data
# pattern <- "(data|dt).*extracao"
# var_name <- "dt_inicio_certificado"

# Função ####
uniformiza_nome <- function(x, pattern, var_name) {

  pattern <- pattern %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    tolower()
  
  position_var <- x %>% 
    names() %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    tolower() %>% 
    str_detect(pattern) %>% 
    which() 
  
  if(length(position_var) > 0 ) {
    
    names(x)[position_var[[1]] ] <- var_name
    
  }
  
  return(x)
  rm(x, position_var, var_name, pattern)
}

# Fim ####