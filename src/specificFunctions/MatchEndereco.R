
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

geolocalizacao_osc <- tibble() %>% 
  mutate(cnpj = NA_character_, 
         COD_MUNICIPIO = NA_character_, 
         id_endereco = NA_character_, 
         numero = NA_character_, 
         MatchRua = NA_character_, 
         latitude = NA_real_, 
         longitude = NA_real_)

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
                                                #, verbose = TRUE
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


Corresp_endereco <- tibble()

for (j in seq_along(municipios)) {
  # j <- 2
  
  # 2927408 # Salvador
  # 2905156 # Caetanos
  # which(municipios == 2905156)
  
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
    mutate(id_endereco = referencia, 
           MatchRua = "Match 1")
  
  # nrow(CorrespExata) /  nrow(SemGeoMunic) # Porcentagem de acerto
  
  # Debug:
  # SemGeoMunic %>% 
  #   dplyr::filter(!referencia %in% CorrespExata$referencia) %>% 
  #   View()
  
  # Correspondências aproximadas:
  CorrespAprox <- SemGeoMunic %>% 
    dplyr::filter( !(referencia %in% CorrespExata$referencia)) %>% 
    mutate(MatchRua = NA_character_, 
           id_endereco = NA_character_)
  
  for (i in seq_len(nrow(CorrespAprox))) {
    # Debug:
    # i <- 1
    # message(i)
    # CorrespAprox$razao_social[i]
    # CorrespAprox$Munic_Nome[i]
    # CorrespAprox$CodMunicIBGE[i]
    # CorrespAprox$cep[i]
    # CorrespAprox$logradouro[i]
    # CorrespAprox$referencia[i]
    # filtra_cep$id
    
    # Match 5: Colocar logradouros menores de 4 caracteres automaticamente como
    # match 5 (centroide do município):
    if( nchar(CorrespAprox$logradouro[i]) <= 4 ) {
      CorrespAprox$id_endereco[i] <- CorrespAprox$CodMunicIBGE[i]
      CorrespAprox$MatchRua[i] <- "Match 5"
      next
    }

    # Match 2: mesmo município, mesmo cep, pequena divergência de endereço.
    # (Distância máxima de 5)
    filtra_cep <- ruasMunic %>% 
      dplyr::filter(COD_MUNICIPIO == CorrespAprox$CodMunicIBGE[i]) %>% 
      dplyr::filter(CEP == CorrespAprox$cep[i]) %>% 
      select(everything())
          
    if(nrow(filtra_cep) > 0) {
      
      teste_distancia <- adist(CorrespAprox$referencia[i], 
                               filtra_cep$id, 
                               ignore.case = TRUE) %>% 
        as.data.frame() %>% 
        transpose() %>% 
        mutate(flag = V1 == min(V1, na.rm = TRUE))
      
      # Distância máxima de 5:
      if(min(teste_distancia$V1, na.rm = TRUE)[[1]] <= 5) {
        CorrespAprox$id_endereco[i] <- filtra_cep$id[which(teste_distancia$flag)[[1]]]
        CorrespAprox$MatchRua[i] <- "Match 2"
        next
      }
      rm(teste_distancia)
    }
    rm(filtra_cep)

    # Match 3: mesmo município, mesmo endereço, pequenas divergências no CEP.
    # (Distância máxima de 2)
    
    mesmo_endereco <- ruasMunic %>% 
      dplyr::filter(CleanAdress == CorrespAprox$CleanAdress[i])
    
    if(nrow(mesmo_endereco) > 0) {
      
      teste_distancia_cep <- adist(CorrespAprox$cep[i], 
                                   mesmo_endereco$CEP, 
                                   ignore.case = TRUE) %>% as.integer()
      
      # Distância máxima de 2:
      if(min(teste_distancia_cep, na.rm = TRUE)[[1]] <= 2) {
        
        # Encontra o id mais próximo, dentro do cep mais próximo:
        melhor_cep_id <- mesmo_endereco$id %>% 
          magrittr::extract(teste_distancia_cep == min(teste_distancia_cep, na.rm = TRUE))
        
        teste_distancia_id <- adist(CorrespAprox$referencia[i], 
                                    melhor_cep_id, 
                                    ignore.case = TRUE) %>% as.integer()
        
        CorrespAprox$id_endereco[i] <- melhor_cep_id %>% 
          magrittr::extract(teste_distancia_id == min(teste_distancia_id, na.rm = TRUE)) %>% 
          magrittr::extract2(1)
        
        CorrespAprox$MatchRua[i] <- "Match 3"
        
        rm(melhor_cep_id, teste_distancia_id)
        rm(teste_distancia_cep, mesmo_endereco)
        next
      }
      rm(teste_distancia_cep)
      
    }
    rm(mesmo_endereco)
    
    #	Match 4: mesmo município, cep + endereço sem o tipo e máximo de 10 caracteres.
    # (Distância máxima de 2)
    
    NovaReferencia <- CorrespAprox$referencia[i] %>% 
      str_remove(CorrespAprox$tipo_logradouro[i]) %>% 
      str_sub(1, 18)
    
    id2 <- ruasMunic %>% 
      mutate(id2 = str_remove(id, NOM_TIPO_SEGLOGR), 
             id2 = str_sub(id2, 1, 18)) %>% 
      select(id2) %>% unlist() %>% as.character()
    
    
    teste_distancia_4 <- adist(NovaReferencia, 
                               id2, 
                               ignore.case = TRUE) %>% as.integer()
    
    # Distância máxima de 2:
    if(min(teste_distancia_4, na.rm = TRUE)[[1]] <= 2) {
      
      CorrespAprox$id_endereco[i] <- teste_distancia_4 %>% 
        min(na.rm = TRUE) %>% 
        magrittr::equals(teste_distancia_4) %>% 
        which() %>% 
        magrittr::extract2(1) %>% 
        ruasMunic$id[.]
      
      CorrespAprox$MatchRua[i] <- "Match 4"
      rm(teste_distancia_4, NovaReferencia, id2)
      next
    }
    rm(teste_distancia_4, NovaReferencia, id2)
    
    
    # Match 5: se tudo acima falhou, colocar match 5:
    CorrespAprox$id_endereco[i] <- CorrespAprox$CodMunicIBGE[i]
    CorrespAprox$MatchRua[i] <- "Match 5"
  }
  rm(i)
  
  
  Corresp_endereco <- Corresp_endereco %>% 
    bind_rows(CorrespExata) %>% 
    bind_rows(CorrespAprox)
}


# Adicionar  Latitude e Longitude onde houve match # TO DO
# Adicionar  Latitude e Longitude no match 5 (endereço) # TO DO

# Processar casos match5
geolocalizacao_osc <- tibble() %>% 
  mutate(cnpj = NA_character_, 
         COD_MUNICIPIO = NA_character_, 
         id_endereco = NA_character_, 
         numero = NA_character_, 
         precisao = NA_character_, 
         latitude = NA_real_, 
         longitude = NA_real_)

match5 <- Corresp_endereco %>% 
  dplyr::filter(MatchRua == "Match 5") %>% 
  mutate(COD_MUNICIPIO = as.character(CodMunicIBGE), 
         cnpj = str_pad(as.character(cnpj), 
                        side = "left", 
                        pad = "0", 
                        width = 14)) %>% 
  select(cnpj, COD_MUNICIPIO, id_endereco, numero, MatchRua) %>% 
  left_join(munic_geo, by = "COD_MUNICIPIO") %>% 
  rename(latitude = LATITUDE, 
         longitude = LONGITUDE, 
         precisao = MatchRua)


geolocalizacao_osc <- bind_rows(geolocalizacao_osc, match5)
rm(match5)

# Processar casos SN:

# Casos em que aparece "SN" (ou qualquer conjunto apenas numério), no número
Corresp_endereco <- Corresp_endereco %>% 
  dplyr::filter(MatchRua != "Match 5") %>% 
  mutate(numeroclean = str_remove_all(numero, "[:punct:]"), # Remove pontuação
         numeroclean = toupper(numeroclean),
         numero_num = str_remove_all(numeroclean, "[:alpha:]"), # remove caracteres
         numero_num = as.numeric(numero_num),
         # Será sem número (SN) se o número for zero ou sem dígitos.
         SN = ifelse(is.na(numero_num) | numero_num < 1, 1, 0)
         )


matchSN <- Corresp_endereco %>% 
  dplyr::filter(MatchRua != "Match 5", 
                SN == 1) %>%
  mutate(COD_MUNICIPIO = as.character(CodMunicIBGE), 
         cnpj = str_pad(as.character(cnpj), 
                        side = "left", 
                        pad = "0", 
                        width = 14), 
         precisao = paste0(MatchRua, "SN")) %>% 
  select(cnpj, COD_MUNICIPIO, id_endereco, numero, precisao) %>% 
  left_join(select(rua_geo, id, LATITUDE, LONGITUDE), 
            by = c("id_endereco" = "id")) %>% 
  rename(latitude = LATITUDE, 
         longitude = LONGITUDE) %>% 
  select(everything())

# Casos em que o número estoura o limte dos números da rua:
limite_endereco <- endereco_geo %>% 
  group_by(id, COD_MUNICIPIO) %>% 
  summarise(max_num = max(NUM_ENDERECO, na.rm = TRUE)) %>% 
  mutate(max_num = max_num + 100)

matchSN_lim <- Corresp_endereco %>% 
  dplyr::filter(MatchRua != "Match 5", 
                SN == 0) %>%
  mutate(COD_MUNICIPIO = as.character(CodMunicIBGE), 
         cnpj = str_pad(as.character(cnpj), 
                        side = "left", 
                        pad = "0", 
                        width = 14), 
         precisao = paste0(MatchRua, "SN")) %>% 
  select(cnpj, COD_MUNICIPIO, id_endereco, numero_num, precisao) %>% 
  left_join(limite_endereco, 
            by = c("id_endereco" = "id", 
                   "COD_MUNICIPIO" = "COD_MUNICIPIO")) %>% 
  mutate(SN_maxLim = ifelse(numero_num > max_num, 1, 0)) %>% 
  dplyr::filter(SN_maxLim == 1) %>% 
  select(cnpj, COD_MUNICIPIO, id_endereco, numero_num, precisao) %>% 
  left_join(select(rua_geo, id, COD_MUNICIPIO, LATITUDE, LONGITUDE), 
            by = c("id_endereco" = "id",
                   "COD_MUNICIPIO" = "COD_MUNICIPIO"), 
            multiple = "first") %>% 
  rename(latitude = LATITUDE, 
         longitude = LONGITUDE, 
         numero = numero_num) %>% 
  mutate(numero = as.character(numero)) %>% 
  select(everything())

geolocalizacao_osc <- bind_rows(geolocalizacao_osc, matchSN, matchSN_lim)
rm(matchSN, limite_endereco, matchSN_lim)


# Encontrar numeração correspondente. # TO DO

NumeroExato <- Corresp_endereco %>% 
  mutate(COD_MUNICIPIO = as.character(CodMunicIBGE), 
         cnpj = str_pad(as.character(cnpj), 
                        side = "left", 
                        pad = "0", 
                        width = 14), 
         precisao = paste0(MatchRua, "EX")) %>%  
  dplyr::filter( !(cnpj %in% geolocalizacao_osc$cnpj) ) %>%
  select(cnpj, COD_MUNICIPIO, id_endereco, numero_num, precisao) %>% 
  mutate(latitude = NA_real_, 
         longitude = NA_real_)

# Procura a melhor correspondência de número no banco:
for (g in seq_len(nrow(NumeroExato))) {
  # g <- 19
  
  message(g, " de ", nrow(NumeroExato))
  
  # Selecionar as localizações do id_endereco
  endereco_geo_g <- endereco_geo %>% 
    dplyr::filter(id == NumeroExato$id_endereco[g]) %>% 
    mutate(distancia = NUM_ENDERECO - NumeroExato$numero_num[g]) %>% 
    arrange(desc(NUM_ENDERECO))
  
  # Encontrar o número de menor distância (que pode ser o exato número)
  menor_distancia <- endereco_geo_g$distancia %>% 
    abs() %>%
    magrittr::equals(., min(., na.rm = TRUE)) %>% 
    which() 
  
  NumeroExato$latitude[g] <- endereco_geo_g$LATITUDE[menor_distancia][[1]]
  NumeroExato$longitude[g] <- endereco_geo_g$LONGITUDE[menor_distancia][[1]]
  
  rm(menor_distancia, endereco_geo_g)
  
}

NumeroExato <- NumeroExato %>% 
  mutate(numero = as.character(numero_num)) %>% 
  select(-numero_num)
  

geolocalizacao_osc <- bind_rows(geolocalizacao_osc, NumeroExato)
rm(NumeroExato)


table(geolocalizacao_osc$precisao)

geolocalizacao_osc %>% 
  group_by(precisao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = Freq / sum(Freq))

# Fim ####



