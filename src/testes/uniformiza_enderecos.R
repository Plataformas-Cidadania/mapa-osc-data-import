# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: uniformizar e remover inconsistências de endereços das
# bases da Recebira Federal (RFB) e do IBGE (CNEFE)

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Script: 2024-10-24


# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(glue)
library(stringr)
library(stringi)
library(lubridate)
library(assertthat)
library(readxl)

# Debug:

# tb_corrige_enderecos <- read_xlsx("tab_auxiliares/tb_corrige_enderecos.xlsx", 1)
# x <- agrega_enderecos$NOM_SEGLOGR

uniformiza_enderecos <- function(x, tb_corrige_enderecos) {
  
  # Algumas verificações para evitar erros:
  assert_that(is.character(x))
  assert_that("data.frame" %in% class(tb_corrige_enderecos))
  assert_that(
    all(
      c("Expressao", "Substituicao", "Acao", 
        "EntreEspacos", "Ordem") %in% names(tb_corrige_enderecos)
      ))
  
  
  # Formata x (endereços)
  x <- x %>% 
    # Remove espaços excessivos
    str_squish() %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    toupper()
  
  
  # Formata expressão de busca
  tb_corrige_enderecos$Expressao <- tb_corrige_enderecos$Expressao %>% 
    # Remove interpretação literal de duas barras
    str_replace_all(fixed("\\\\"), "\\") %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    toupper()
  
  # Formata expressão de substituição
  tb_corrige_enderecos$Substituicao <- tb_corrige_enderecos$Substituicao %>% 
    # Remove interpretação literal de duas barras
    str_replace_all(fixed("\\\\"), "\\") %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    toupper()
  
  for (h in seq_len(nrow(tb_corrige_enderecos))) {
    # Debug:
    # h <- 19
    # tb_corrige_enderecos[h, ]
    # print(h)
    
    # Corrige expressão de busca, caso entre espaços 
    # (ex: não mudar 'UMBERTO' para 1BERTO, mas  'UM DE JANEIRO', para '1 DE JANEIRO')
    if(tb_corrige_enderecos$EntreEspacos[h] == 1) {
      x <- paste0(" ", x) # Evita dar erro com expressões no início
      expressao_busca <- paste0("(?<=[:space:])", tb_corrige_enderecos$Expressao[h], 
                                "(?=[:space:])")
      
      # Corrige a expressão regular '^' (início da expressão)
      if(str_sub(tb_corrige_enderecos$Expressao[h], 1, 1) == "^") {
        
        expressao_busca <- expressao_busca %>% 
          str_remove(fixed("^")) %>%
          str_remove(fixed("(?<=[:space:])")) %>% 
          paste0("^ ", .)
        
      }
      
    } else {
      expressao_busca <- tb_corrige_enderecos$Expressao[h]
    }
    # expressao_busca # Debug
    
    # Quando o ajuste for 'replace':
    if(tb_corrige_enderecos$Acao[h] == "replace") {

      substituirpor <- tb_corrige_enderecos$Substituicao[h] %>% 
        # Uso essa expressão para espaço em branco, que não é bem compreendido na tabela:
        str_replace(fixed("[:SPACE:]"), " ")
            
      x <- str_replace(x, expressao_busca, substituirpor)
      
      rm(substituirpor)
    }
    
    # Quando o ajuste for 'remove':
    if(tb_corrige_enderecos$Acao[h] == "remove") {
      x <- str_remove(x, expressao_busca)
    }
    
    # Debug: 
    # sum(str_detect(x, expressao_busca))
    # which(str_detect(x, expressao_busca))[1:10]
    # x[ which(str_detect(x, expressao_busca))[1:10]  ]
    
    x <- trimws(x) # corrige a adição do espaço quando 'EntreEspacos[h] == 1'
    rm(expressao_busca)
  }
}

# Fim ####