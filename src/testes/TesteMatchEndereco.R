
# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(readxl)
library(utils) # para unzip


# TO DO: pensar em um sistema de teste das regras abaixo:
# Dentro do cep, uma distância menor que 5
# Método 2: entre cep diferentes, distância menor que 2

# Lê os arquivos brutos por Estado:
uf <- "29_BA"


cep_geo <- readRDS(glue("tab_auxiliares/latlon_enderecos/{uf}_cep_geo.RDS"))
endereco_geo <- readRDS(glue("tab_auxiliares/latlon_enderecos/{uf}_endereco_geo.RDS"))
munic_geo <- readRDS(glue("tab_auxiliares/latlon_enderecos/{uf}_munic_geo.RDS"))
rua_geo <- readRDS(glue("tab_auxiliares/latlon_enderecos/{uf}_rua_geo.RDS"))

rua_geo$id <- rua_geo$id %>% 
  str_remove_all(" ") %>% 
  str_remove_all("_")

EndSemGeo$referencia <- EndSemGeo$referencia %>% 
  str_remove_all(" ") %>% 
  str_remove_all("_")

endereco_geo$id <- endereco_geo$id %>% 
  str_remove_all(" ") %>% 
  str_remove_all("_")


# Arquivo com endereços para se fazer o match:
Galileo_Raw <- fread("backup_files/2024_01/intermediate_files/input_busca_geo.csv", 
                     encoding = "Latin-1")

# names(Galileo_Raw)

# Expressões de Correção de Endereços:
tb_corrige_enderecos <- readxl::read_xlsx("tab_auxiliares/tb_corrige_enderecos.xlsx", 1)

# Função Para Corrigir Endereços:
source("src/generalFunctions/uniformiza_enderecos.R")


Galileo_Raw$CleanAdress <- uniformiza_enderecos(Galileo_Raw$logradouro, 
                                                tb_corrige_enderecos
                                                , verbose = TRUE
                                                )


# Seleciona uma amostra para teste
EndSemGeo <- Galileo_Raw %>% 
  mutate(UF = str_sub(CodMunicIBGE, 1, 2)) %>%  # Cria a variável UF
  # dplyr::filter(Munic_Nome == "BELÉM") %>%
  dplyr::filter(UF == str_remove(uf, "_.*")) %>%
  mutate(referencia = paste0(cep, tipo_logradouro, CleanAdress), 
         referencia = stringi::stri_trans_general(referencia, id = "Latin-ASCII"), 
         referencia = str_remove_all(referencia, "[:punct:]"), 
         referencia = str_remove_all(referencia, " ")) %>% 
  select(everything())


municipios <- unique(EndSemGeo$CodMunicIBGE)



for (j in seq_along(municipios)) {
  # j <- 2
  
  # 2927408 # Salvador
  
  message(municipios[j])
  
  # names(endereco_geo)
  
  SemGeoMunic <- EndSemGeo %>%
    dplyr::filter(CodMunicIBGE == municipios[j]) 
  
  ruasMunic <- rua_geo %>% 
    dplyr::filter(COD_MUNICIPIO == municipios[j]) 
  
  # enderecosMunic <- endereco_geo %>% 
  #   dplyr::filter(COD_MUNICIPIO == municipios[j]) 
  
  

  
  # Match 1: mesmo município, correspondências Exata cep + endereço:
  CorrespExata <- SemGeoMunic %>%
    dplyr::filter(referencia %in% ruasMunic$id) %>% 
    mutate(id_endereco = referencia)
  
  nrow(CorrespExata) /  nrow(SemGeoMunic) # Porcentagem de acerto
  
  # ruasMunic$id[1:50]
  # SemGeoMunic$referencia[1:50]
  
  SemGeoMunic %>% 
    dplyr::filter(!referencia %in% CorrespExata$referencia) %>% 
    View()
  
  ruasMunic$id[492]
  SemGeoMunic$referencia[38]
  
  # Nese caso, a numeração confundiu
  # 40080003AVENIDASETEDESETEMBRO
  # 40080002AVENIDA7DESETEMBRO
  
  # Nesse caso, a pessoa colocou o cep errado (não tem esse CEP nesta avenida)
  # tem um CEP que passou perto
  # 40210904AVENIDAANITAGARIBALDI
  # 40210750AVENIDAANITAGARIBALDI
  
  # Aqui a pessoa colocaou avenida duas vezes:
  # 41635150AVENIDAAVENIDADORIVALCAYMMI
  # 41635150AVENIDADORIVALCAYMMI
  
  # Aqui a pessoa colocou o nome do prédio no endereço
  # 41730101AVENIDALUISVIANAFILHOWALLSTREETEMPRESARIAL
  
  # Existem três CEPs com essa avenida e nenhum é esse, não passou nem perto
  # 41820915AVENIDATANCREDONEVES
  
  # Nesse caso, a pessoa confundiu um número do CEP
  # 41825900AVENIDAANTONIOCARLOSMAGALHAES
  # 41825000AVENIDAANTONIOCARLOSMAGALHAES
  

  # ruasMunic$id[str_detect(ruasMunic$id, "AVENIDAANITAGARIBALDI")]
  
  teste <- endereco_geo %>% 
    dplyr::filter(id == "40210750AVENIDAANITAGARIBALDI") 
  
  
  # Correspondências aproximadas:
  CorrespAprox <- SemGeoMunic %>% 
    dplyr::filter( !(referencia %in% CorrespExata$referencia)) %>% 
    mutate(distancia_match2 = NA_integer_)
  
  for (i in seq_len(nrow(CorrespAprox))) {
    # i <- 2
    
    message(i)
    
    CorrespAprox$CodMunicIBGE[i]
    CorrespAprox$cep[i]
    CorrespAprox$logradouro[i]
    
    # Match 2: mesmo município, mesmo cep, pequena divergência de endereço.
    
    filtra_cep <- rua_geo %>% 
      dplyr::filter(COD_MUNICIPIO == CorrespAprox$CodMunicIBGE[i]) %>% 
      dplyr::filter(CEP == CorrespAprox$cep[i])
      select(everything())
      
      if(nrow(filtra_cep) == 0) {
        message("CEP não encontrado")
        next
      }
    
    # Estou aqui !!!!####

    
    teste_distancia <- adist(CorrespAprox$referencia[i], 
                             filtra_cep$id, 
                             ignore.case = TRUE) %>% 
      as.data.frame() %>% 
      transpose() %>% 
      mutate(flag = V1 == min(V1))


    CorrespAprox$distancia_match2[i] <- min(teste_distancia$V1, na.rm = TRUE)[[1]]
    
    rm(teste_distancia)
    
    # Criar aqui uma rotina para distância máxima # TO DO
    
    CorrespAprox2$id_endereco[i] <- teste_distancia  %>% 
      select(flag) %>% 
      unlist() %>% 
      as.logical() %>% 
      which() %>% 
      magrittr::extract2(1) %>% 
      filtra_cep$id[.]
    
    rm(filtra_cep, teste_distancia)
  }
  rm(i)
  
  
  
}




# TO DO: colocarf aqui a forma de encontrar a referência


# Etapa 3: menor distância:
CorrespAprox2 <- EndSemGeo %>% 
  dplyr::filter( !(referencia %in% CorrespExata$referencia),
                 !(referencia %in% CorrespAprox1$referencia) ) 

nrow(CorrespAprox2)


nrow(CorrespAprox2)

CorrespAprox2$id_endereco <- NA_character_
CorrespAprox2$distancia_n <- NA_integer_



EndSemGeo$referencia %in% latloncep$referencia1 %>% mean()


EndSemGeo$referencia %in% latloncep$referencia2 %>% mean()


EndSemGeo$cep[1]

names(data_teste3)

data_teste4 <- data_teste3 %>% 
  dplyr::filter(CEP == "68743020") %>% 
  #dplyr::filter(str_detect(NOM_SEGLOGR, "LUIS")) %>% 
  #dplyr::filter(str_detect(NOM_SEGLOGR, "LEITAO")) %>%
  select(everything())

EndSemGeo$logradouro[1]
data_teste4$Endereco[10]

library(stringdist)

adist(EndSemGeo$logradouro[1], 
      unique(data_teste4$Endereco), 
      ignore.case = TRUE) %>% 
  as.data.frame() %>% 
  transpose() %>% 
  mutate(flag = V1 == min(V1)) %>% 
  select(flag) %>% 
  unlist() %>% 
  as.logical() %>% 
  which() %>% 
  magrittr::extract2(1)
  

?adist

# Workflow:

# Uma função que encontra um endereço encontrado em RFB com um grupo de 
# endereços da CNEFE.

## Filtra por CEP
## Remove tipos de logradoutro, se houver

## Tenta encontrar a correspondência exata:
### de título + nome
### Sem o título

## Caso a etapa anterior não der certo, calcular a distância textual:
### de título + nome
### Sem o título

## Aceitar o caso da menor distância, desde que:
### A distância não for menor que 5


# Fim ####



