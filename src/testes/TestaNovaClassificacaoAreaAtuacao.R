# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: testar uma forma de criar uma classificação da área de atuação OSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-10-04

library(readxl)
library(lubridate)
library(stringr)

# Função para determinar as áreas de atuação
source("src/specificFunctions/AreaAtuacaoOSC.R")


Tb_OSC_Full <- readRDS("backup_files/2024_01/intermediate_files/Tb_OSC_Full.RDS")

# names(Tb_OSC_Full)

# Transforma Tb_OSC_Full em DB_OSC
DB_OSC <- Tb_OSC_Full %>%
  mutate(cnae = .data[["cnae_fiscal"]], 
  # mutate(cnae = .data[["cnae_fiscal_principal"]], 
         micro_area_atuacao = NA)

# DB_OSC <- slice(DB_OSC, 1:100000)

# names(DB_OSC)

# DB_AreaSubaria <- fread("tab_auxiliares/Areas&Subareas.csv",
#                         encoding = "Latin-1")

# Regras para determinar as subáreas de atuação
DB_SubAreaRegras <- read_xlsx("tab_auxiliares/IndicadoresAreaAtuacaoOSC.xlsx", 
                              sheet = 1)

# Relação entre micro áreas e macro áreas
DB_AreaSubaria <- read_xlsx("tab_auxiliares/IndicadoresAreaAtuacaoOSC.xlsx", 
                            sheet = "Areas&Subareas")

# names(DB_SubAreaRegras)

# DB_SubAreaRegras <- DB_SubAreaRegras %>% 
#   dplyr::filter(AreaAtuacao != "Desenvolvimento rural")

# rm(Tb_OSC_Full) # não vamos mais utilizar esses dados



# Usa função "AreaAtuacaoOSC" para determinar qual a área de atuação
# das OSCs
DB_OSC$micro_area_atuacao <- AreaAtuacaoOSC(select(DB_OSC, 
                                                   cnpj_basico, 
                                                   razao_social, 
                                                   cnae, 
                                                   micro_area_atuacao), 
                                            slice(DB_SubAreaRegras, 1:323), 
                                            chuck_size = 10000, verbose = FALSE)

DB_OSC <- DB_OSC %>% 
  # Se não foi indentificado pelo sistema, colocar "Outras"
  mutate(micro_area_atuacao = ifelse(is.na(micro_area_atuacao), 
                                     "Outras organizações da sociedade civil", 
                                     micro_area_atuacao)) %>% 
  # Insere Macro áreas de atuação
  left_join(DB_AreaSubaria, by = "micro_area_atuacao")

# now()

DB_OSC %>% 
  group_by(micro_area_atuacao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = Freq/sum(Freq)) %>% 
  print(n = 40)

# DB_OSC$razao_social[DB_OSC$micro_area_atuacao == "Outras formas de desenvolvimento e defesa de direitos"] %>% 
#   sample(20)


# Comparativo entre o critério anterior e o presente:

# Baixa dados da última atualização:

getwd()

setwd(rstudioapi::getActiveProject())

tb_area_atuacao_2024 <- readRDS("backup_files/2024_01/output_files/tb_area_atuacao.RDS")

## Tabelas auxiliares:
dc_area_atuacao <- fread("tab_auxiliares/dc_area_atuacao.csv", encoding = "Latin-1")
dc_subarea_atuacao <- fread("tab_auxiliares/dc_subarea_atuacao.csv", encoding = "Latin-1") %>% 
  distinct(cd_subarea_atuacao, .keep_all = TRUE)
# names(dc_subarea_atuacao)

# Compara dados de área de atuação:

JoinData <- tb_area_atuacao_2024 %>% 
  dplyr::filter(ft_area_atuacao == "AreaAtuacaoOSC.R_2024_01") %>% 
  left_join(dc_area_atuacao, by = "cd_area_atuacao") %>% 
  group_by(tx_area_atuacao) %>% 
  summarise(Freq_OLD = n()) %>% 
  mutate(Per_OLD = (Freq_OLD/sum(Freq_OLD)) * 100)

# names(DB_OSC)

AreaTable <- DB_OSC %>% 
  rename(tx_area_atuacao = macro_area_atuacao) %>% 
  group_by(tx_area_atuacao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = (Freq/sum(Freq)) * 100) %>% 
  full_join(JoinData, by = "tx_area_atuacao") %>% 
  mutate(Diferenca = Freq - Freq_OLD)

AreaTable


# Compara dados de área de SubArea de atuação:

JoinData <- tb_area_atuacao_2024 %>% 
  dplyr::filter(ft_area_atuacao == "AreaAtuacaoOSC.R_2024_01") %>% 
  dplyr::filter( !is.na(cd_subarea_atuacao) ) %>% 
  left_join(dc_subarea_atuacao, by = "cd_subarea_atuacao") %>% 
  group_by(tx_subarea_atuacao) %>% 
  summarise(Freq_OLD = n()) %>% 
  mutate(Per_OLD = (Freq_OLD/sum(Freq_OLD)) * 100)

# names(DB_OSC)

SubAreaTable <- DB_OSC %>% 
  rename(tx_subarea_atuacao = micro_area_atuacao) %>% 
  group_by(tx_subarea_atuacao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = (Freq/sum(Freq)) * 100) %>% 
  full_join(JoinData, by = "tx_subarea_atuacao") %>% 
  mutate(Diferenca = Freq - Freq_OLD)

SubAreaTable %>% 
  print(n = 40)


# Compara dados de área de Defesa de Direitos:

JoinData <- tb_area_atuacao_2024 %>% 
  dplyr::filter(ft_area_atuacao == "AreaAtuacaoOSC.R_2024_01") %>% 
  dplyr::filter( cd_area_atuacao == 9 ) %>% 
  left_join(dc_subarea_atuacao, by = "cd_subarea_atuacao") %>% 
  group_by(tx_subarea_atuacao) %>% 
  summarise(Freq_OLD = n()) %>% 
  mutate(Per_OLD = (Freq_OLD/sum(Freq_OLD)) * 100)

# names(DB_OSC)

table(DB_OSC$macro_area_atuacao)

DefesaDireitos <- DB_OSC %>% 
  rename(tx_subarea_atuacao = micro_area_atuacao) %>% 
  dplyr::filter(macro_area_atuacao == "Desenvolvimento e defesa de direitos e interesses") %>% 
  group_by(tx_subarea_atuacao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = (Freq/sum(Freq)) * 100)

DefesaDireitos %>% 
  print(n = 40)

# Salva dados:

fwrite(AreaTable, 
       "D:/Users/Murilo/Dropbox/Trabalho/IPEA/SubProjetos/2024-08 Fasfil/AreaTable.csv",
       sep = ";", dec = ",")

fwrite(SubAreaTable, 
       "D:/Users/Murilo/Dropbox/Trabalho/IPEA/SubProjetos/2024-08 Fasfil/SubAreaTable.csv",
       sep = ";", dec = ",")

fwrite(DefesaDireitos, 
       "D:/Users/Murilo/Dropbox/Trabalho/IPEA/SubProjetos/2024-08 Fasfil/DefesaDireitos.csv",
       sep = ";", dec = ",")




# Fim ####