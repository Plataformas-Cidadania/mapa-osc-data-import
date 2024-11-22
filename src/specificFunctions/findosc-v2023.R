# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: criar uma função  para selecionar as OSC dentro dos 
# CNPJ da Receita Federal do Brasil (RFB), excluindo organizações que obviamente,
# não são OSC com base em uma lista de expressões regulares. 

# Cria um procedimento para não esgotar a memória do computador, através de uma
# estratégia de "dividir e conquistar".

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2023-10-19

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# x: vetor de texto da razão social das OSC
# NonOSCNames: banco de dados com as regras para se excluir razões sociais não OSC

# Parâmetros opcionais:
# chuck_size: tamanho de grupos de x a ser avaliados por vez
# size_detect: tamanho de expressões regulares a ser avaliadas por vez
# verbose: para a função mostrar mais informações durante a execução

## Funções auxiliares:
# "src/generalFunctions/str_detect_split.R"

## Outputs:
# vetor lógico (TRUE, FALSE), sendo FALSE identificada como não OSC.

# bibliotecas necessárias:
library(tidyverse)
library(stringr)
library(stringi)
library(assertthat)
library(lubridate)

# Debug: ####
# x <- tb_JoinOSC$razao_social
# verbose = TRUE
# chuck_size = 5000
# size_detect = 20

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# find_OSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


find_OSC <- function(x, NonOSCNames, 
                     chuck_size = 5000, size_detect = 20, verbose = FALSE){

  # Garante que x é texto
  assert_that(is.character(x))
  assert_that(length(x) > 0)
  
  # Garante que NonOSCNames é data frame e tem as variáveis especificadas
  assert_that("tbl_df" %in% class(NonOSCNames))
  assert_that(all(c("Expressao", "GrupoMultiplo", "IndicaNaoOSC", 
                    "SubGrupo") %in% names(NonOSCNames)))
  assert_that(nrow(NonOSCNames) > 0)
  
  # Função str_detect_split para optimizar os testes com expressões regulares
  source("src/generalFunctions/str_detect_split.R", local = TRUE)
  
  # Verifica se está tudo certo com a função str_detect_split
  assert_that(exists("str_detect_split"))
  assert_that(is.function(str_detect_split))
  
  # Formata x (razão social dos estabelecimentos)
  x <- x %>% 
    # Remove espaços excessivos
    str_squish() %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    toupper()
  
  # Formata NonOSCNames (nomes que não são OSC)
  NonOSCNames$Expressao <-NonOSCNames$Expressao %>% 
    # Remove interpretação literal de duas barras
    str_replace_all(fixed("\\\\"), "\\") %>% 
    # Remove acentos
    stri_trans_general("Latin-ASCII") %>% 
    # Para maiúsculo
    toupper()
  
  # Separa os CNPJ em grupos (tamanho default 5000)
  split_CNPJ <- split(x, ceiling(seq_along(x)/chuck_size))
  rm(x)
  
  # length(split_CNPJ)
  
  # Critério de exclusão simples (não estar em NonOSCNames$Expressao)
  ExcludeName <- NonOSCNames %>% 
    dplyr::filter(GrupoMultiplo == 0) %>% 
    select(Expressao) %>% unlist() %>% as.character()
  
  # Grupos de critérios multiplos:
  GrupoMultiplos <- NonOSCNames$GrupoMultiplo %>% 
    magrittr::extract(. > 0) %>% 
    unique()

  # BD para guardar o resultado da busca
  gatherCheck <- tibble()
  
  # Exclui as razões sociais segundo os critérios especificados:
  for (i in seq_along(split_CNPJ)) {
    # i <- 1
    
    message("Conjunto de nomes ", i, "/", length(split_CNPJ))
    
    # Teste de grupos de 5000 Nomes
    NewData <- enframe(split_CNPJ[[i]], 
                       name = NULL, 
                       value = "razao_social") %>% 
      mutate(IsOSC = TRUE)
    
    # Exclui todas as razões sociais que estão dentro do critério simples de
    # exclusão:
    NewData$IsOSC <- NewData$IsOSC & !str_detect_split(split_CNPJ[[i]], 
                                                       ExcludeName, 
                                                       chunksize = size_detect)
    # sum(NewData$IsOSC)
    # NewData$razao_social[!NewData$IsOSC]
    
    # Seleção com base em múltiplos critérios
    for (h in seq_along(GrupoMultiplos)) {
      # h <- 1
      
      if (verbose) message("Teste grupo múltiplo ", h)
      
      # Dados do critério múltiplo em teste:
      NamesGrupo <- NonOSCNames %>% 
        dplyr::filter(GrupoMultiplo == GrupoMultiplos[h])
      
      # Vetor com os resultados dos testes
      test_OSC <- rep(TRUE, length(split_CNPJ[[i]]))
      
      # sum(test_OSC)
      # NewData$razao_social[test_OSC]
      
      # Garante que exista subgrupos no critério múltiplo
      assert_that(
        any(str_detect(NamesGrupo$SubGrupo, "[123456789]")), 
        msg = "Não foi encontrados Subgrupos no critério múltiplo")
      
      # Subgrupos do critério:
      SubGrupos <- NamesGrupo$SubGrupo %>% 
        magrittr::extract(str_detect(., "[123456789]")) %>%
        as.integer() %>% 
        # Remove NAs
        magrittr::extract(!is.na(.)) %>% 
        # Só valores únicos
        unique()
      
      # Teste dos subgrupos que, combinados, indicam que não é OSC
      # Ou seja (IndicaNaoOSC == 1):
      for(g in SubGrupos) {
        # g <- 2
        check_names <- NamesGrupo %>% 
          dplyr::filter(SubGrupo == g) %>% 
          select(Expressao) %>% 
          unlist() %>%  as.character() 
        
        test_OSC <- test_OSC & str_detect_split(split_CNPJ[[i]], 
                                                check_names, 
                                                chunksize = size_detect)
        rm(check_names)
      }
      rm(g, SubGrupos)

      # Teste das expressões que indicam que é OSC (IndicaNaoOSC == 0):
      IndicaOSC <- NamesGrupo$Expressao[NamesGrupo$IndicaNaoOSC == 0]
      if(length(IndicaOSC) > 0) {
        # Colapsa os nomes
        test_OSC <- test_OSC & !str_detect_split(split_CNPJ[[i]], 
                                                 IndicaOSC, 
                                                 chunksize = size_detect)
      }
      rm(IndicaOSC)

      # atendendo ao critério múltiplo, não é OSC
      NewData$IsOSC <- NewData$IsOSC & !test_OSC 
      rm(test_OSC, NamesGrupo)
      if (verbose) print(sum(NewData$IsOSC))
    }
    rm(h)
    
    # Une ao banco principal
    gatherCheck <- bind_rows(gatherCheck, NewData)
    rm(NewData)
  }
  return(gatherCheck$IsOSC)
  
  rm(i, split_CNPJ, ExcludeName, GrupoMultiplos)
  rm(str_detect_split, gatherCheck)
  rm(chuck_size, verbose, size_detect)
}

# Fim
