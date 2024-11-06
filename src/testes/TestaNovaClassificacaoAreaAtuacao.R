# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: testar uma forma de criar uma classificação da área de atuação OSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-10-04

library(readxl)
library(lubridate)

# Função para determinar as áreas de atuação
source("src/specificFunctions/AreaAtuacaoOSC.R")


Tb_OSC_Full <- readRDS("backup_files/2023_01/intermediate_files/Tb_OSC_Full.RDS")

# names(Tb_OSC_Full)

# Transforma Tb_OSC_Full em DB_OSC
DB_OSC <- Tb_OSC_Full %>%
  # mutate(cnae = .data[["cnae_fiscal"]], 
  mutate(cnae = .data[["cnae_fiscal_principal"]], 
         micro_area_atuacao = NA)

DB_OSC <- slice(DB_OSC, 1:100000)

# names(DB_OSC)

# DB_AreaSubaria <- fread("tab_auxiliares/Areas&Subareas.csv",
#                         encoding = "Latin-1")

# Regras para determinar as subáreas de atuação
DB_SubAreaRegras <- read_xlsx("tab_auxiliares/IndicadoresAreaAtuacaoOSC.xlsx", 
                              sheet = 1)

# Relação entre micro áreas e macro áreas
DB_AreaSubaria <- read_xlsx("tab_auxiliares/IndicadoresAreaAtuacaoOSC.xlsx", 
                            sheet = "Areas&Subareas")

names(DB_SubAreaRegras)

DB_SubAreaRegras <- DB_SubAreaRegras %>% 
  dplyr::filter(AreaAtuacao != "Desenvolvimento rural")

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

now()

DB_OSC %>% 
  group_by(micro_area_atuacao) %>% 
  summarise(Freq = n()) %>% 
  mutate(Per = Freq/sum(Freq)) %>% 
  print(n = 40)

DB_OSC$razao_social[DB_OSC$micro_area_atuacao == "Outras formas de desenvolvimento e defesa de direitos"] %>% 
  sample(20)

# To DO:
# Fazer um comparativo entre o critério anterior e esse aqui.


# Fim ####