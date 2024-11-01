# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Função para determinar a área de atuação da 
# organização a partir do cnae e razão social.

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2023-11-09


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# DB_OSC: dados básicos das OSC
# DB_SubAreaRegras: regras para se determinar as subáreas das OSC
# size_detect: tamanho de expressões regulares a ser avaliadas por vez
# verbose: para a função mostrar mais informações durante a execução


## Funções auxiliares:
# "src/generalFunctions/str_detect_split.R"

## Outputs:
# Um vetor de caracteres contendo a área de atuação da OSC

# bibliotecas necessárias:
library(tidyverse)
library(stringr)
library(stringi)
library(assertthat)
library(lubridate)

# Debug:
# chuck_size = 5000
# verbose = TRUE


AreaAtuacaoOSC <- function(DB_OSC, DB_SubAreaRegras,
                           chuck_size = 5000, verbose = TRUE) {

  assert_that(file.exists("src/generalFunctions/str_detect_split.R"))
  
  # Garante que tenha todas as variáveis em DB_SubAreaRegras
  c("Variavel", "Valor", "AreaAtuacao", "CriterioMultiplo", "IncluiArea",
    "Ordem") %in% names(DB_SubAreaRegras) %>% 
    all() %>% 
    assert_that(msg = "Falta variáveis em DB_SubAreaRegras")

  # Garante que tenha todas as variáveis em DB_OSC
  unique(DB_SubAreaRegras$Variavel) %in% names(DB_OSC) %>% 
    all() %>% 
    assert_that(msg = paste("Falta a(s) variável(is) em DB_OSC:", 
                            unique(DB_SubAreaRegras$Variavel)[!unique(DB_SubAreaRegras$Variavel) %in% names(DB_OSC)]))
  
  assert_that("micro_area_atuacao" %in% names(DB_OSC),
    msg = "Falta variável 'micro_area_atuacao' em DB_OSC")
  
  # Função str_detect_split para optimizar os testes com expressões regulares
  source("src/generalFunctions/str_detect_split.R", local = TRUE)
  
  # Verifica se está tudo certo com a função str_detect_split
  assert_that(exists("str_detect_split"))
  assert_that(is.function(str_detect_split))
  
  # Coloca as colunas em ordem
  DB_SubAreaRegras <- arrange(DB_SubAreaRegras, Ordem)
  
  names(DB_SubAreaRegras)
  
  # Retira problemas de formatação:
  DB_SubAreaRegras <- DB_SubAreaRegras %>% 
    mutate(Valor = as.character(Valor), 
           Valor = str_trim(Valor), 
           Valor = toupper(Valor), 
           # remove acentos:
           Valor = stringi::stri_trans_general(Valor, id = "Latin-ASCII"))
  
  # output
  output <- tibble()
  
  # Separa as linhas de DB_OSC em grupos (tamanho default 5000)
  split_OSC <- seq_len(nrow(DB_OSC)) %>% 
    split(., ceiling(seq_along(.)/chuck_size))
  
  # length(split_OSC[[1]])
  
  # Critérios simples (que não são multiplos)
  SimpleCriteria <- DB_SubAreaRegras %>% 
    dplyr::filter(CriterioMultiplo == 0)
  # View(SimpleCriteria)
  
  # Critérios multiplos
  GrupoMultiplos <- DB_SubAreaRegras %>% 
    select(CriterioMultiplo) %>% 
    dplyr::filter(str_detect(CriterioMultiplo, "[123456789]")) %>% 
    unlist() %>%  as.integer() %>% 
    unique()
  
  for (i in seq_along(split_OSC)) {
    # i <- 2
    
    message("Testando conjunto de OSC ", i, " de ", length(split_OSC))
    
    # recorta uma parte do banco de dados para ser testado de uma vez
    TestData <- DB_OSC %>% 
      slice(split_OSC[[i]]) %>% 
      mutate(TestValor = NA)
    
    # testa critérios simples (que não são multiplos)
    for (j in seq_len(nrow(SimpleCriteria))) {
      # j <- 3
      VariavelJ <- SimpleCriteria$Variavel[j]
      valorJ <- SimpleCriteria$Valor[j]
      AreaJ <- SimpleCriteria$AreaAtuacao[j]
      
      if (verbose) message("Testando ", VariavelJ, " = ", valorJ, " ~ ", AreaJ)
      
      # Testa os dados
      TestData$valorJ <- valorJ
      TestData$AreaJ <- AreaJ
      if(VariavelJ == "cnae") {
        # No caso do cnae, recorta apenas a quantidade de dígitos do teste
        TestData$TestValor <- str_sub(TestData$cnae, 1, nchar(valorJ))
      } else {
        TestData$TestValor <- TestData[[VariavelJ]]
      }
      
      TestData <- TestData %>% 
        mutate(micro_area_atuacao = ifelse(TestValor == valorJ, 
                                           AreaJ, 
                                           micro_area_atuacao))
      
      rm(valorJ, VariavelJ, AreaJ)
    }
    rm(j)
    
    # Seleção com base em múltiplos critérios
    for (h in GrupoMultiplos) {
      # h <- 1
      
      if (verbose) message("Teste grupo múltiplo ", h)
      
      # Dados do critério múltiplo em teste:
      NamesGrupo <- DB_SubAreaRegras %>% 
        dplyr::filter(CriterioMultiplo == h)
      
      AreaAtuacaoH <- NamesGrupo$AreaAtuacao[1]
      
      # Vetor com os resultados dos testes
      test_Area <- rep(TRUE, nrow(TestData))
      
      # Garante que exista subgrupos no critério múltiplo
      assert_that(
        any(str_detect(NamesGrupo$SubgrupoCriterio, "[123456789]")), 
        msg = "Não foi encontrados Subgrupos no critério múltiplo")
      
      # Subgrupos do critério:
      SubGrupos <- NamesGrupo$SubgrupoCriterio %>% 
        magrittr::extract(str_detect(., "[123456789]")) %>%
        as.integer() %>% 
        # Só valores únicos
        unique()
      
      # Teste dos subgrupos 
      for(g in SubGrupos) {
        # g <- 1
        
        if(verbose) message("Testando subgrupo ", h, ".", g)
        
        VarTest <- NamesGrupo %>% 
          dplyr::filter(SubgrupoCriterio == g) %>% 
          select(Variavel) %>%  slice(1) %>% 
          unlist() %>%  as.character() 
        
        check_names <- NamesGrupo %>% 
          dplyr::filter(SubgrupoCriterio == g) %>% 
          select(Valor) %>% 
          unlist() %>%  as.character() 
        
        test_Area <- test_Area & str_detect_split(TestData[[VarTest]], 
                                                check_names)
        # sum(test_Area)
        # sum(!test_Area)
        # TestData$razao_social[test_Area][1:10]
        
        rm(check_names, VarTest)
      }
      rm(g, SubGrupos)
      
      # Teste das expressões que indicam que não é da área acima
      ExcluiArea <- NamesGrupo$Valor[NamesGrupo$IncluiArea == 0]
      
      if(length(ExcluiArea) > 0) {
        VarTest <- NamesGrupo$Variavel[NamesGrupo$IncluiArea == 0][1]
        
        test_Area <- test_Area & !str_detect_split(TestData[[VarTest]], 
                                                   ExcluiArea)
        rm(VarTest)
      }
      rm(ExcluiArea)
      
      # atendendo ao critério múltiplo, é da área
      TestData$micro_area_atuacao[test_Area] <- AreaAtuacaoH
      
      rm(test_Area, NamesGrupo, AreaAtuacaoH)
    }
    rm(h)
    
    output <- bind_rows(output, TestData)
    # table(output, useNA = "always")
  }
  rm(i)
  return(output$micro_area_atuacao)
}

# Fim