# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Responder o pedido de informações do BDSP, TV Globo, 
# feito no dia 2024-10-14

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-10-14



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(readxl)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega Dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


tb_localizacao <- readRDS("backup_files/2023_01/output_files/tb_localizacao.RDS")
RM_SP <- read_xlsx("data/temp/RM_SP.xlsx", 1)
tb_JoinOSC <- readRDS("backup_files/2023_01/intermediate_files/tb_JoinOSC.RDS")
CodMunicRFB <- fread("tab_auxiliares/CodMunicRFB.csv")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Perguntas 01: Quantas ONGs em todo estado de SP? ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


names(tb_localizacao)

tb_localizacao %>% 
  mutate(cd_uf = str_sub(cd_municipio, 1, 2)) %>% 
  dplyr::filter(cd_uf == "35") %>% 
  nrow()

# 178368


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Perguntas 02: Quantas ONGs são na capital paulista? ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tb_localizacao %>% 
  dplyr::filter(cd_municipio == "3550308") %>% 
  nrow()

# 55064

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Perguntas 03: Quantas são na RMSP? ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


tb_localizacao %>% 
  dplyr::filter(cd_municipio %in% RM_SP$cd_municipio) %>% 
  nrow()


# 87022



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Perguntas 04: Quantas ONGs encerraram suas atividades entre 2021-01-01 e 2024-09-01 ? ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

names(CodMunicRFB)

CodMunicRFB <- mutate(CodMunicRFB, CodMuniRFB = as.character(CodMuniRFB))

# Extrai data de Fechamento das OSC ####
tb_JoinOSC <- tb_JoinOSC %>% 
  rename(CodMuniRFB = municipio) %>% 
  mutate(CodMuniRFB = as.character(CodMuniRFB)) %>% 
  left_join(CodMunicRFB, by = "CodMuniRFB") %>% 
  # Campo de situação a atividade da OCS:
  mutate(situacao = as.integer(situacao_cadastral), 
         bo_osc_ativa = situacao %in% c(2, 3, 4), 
         # Existem casos em que a data_situacao_cadastral é '0', mesmo em OSC inativas.
         # Investiguei todos esses casos e me parece que são falhas de versões anteriores 
         # do find_osc, pois não me parecem OSC pelos critérios adotados. Vou excluir essas
         # OSC da amostra (são 1704 casos).
         data_situacao_cadastral = ifelse(data_situacao_cadastral == 0, 
                                          NA, data_situacao_cadastral),
         
         # A data de fechamento da OSC é a data da situação cadastral quando
         # ela está inativa:
         dt_fechamento_osc = ifelse(bo_osc_ativa, NA, 
                                    data_situacao_cadastral), 
         # Ano de fechamento OSC
         nr_ano_fechamento_osc = year(ymd(dt_fechamento_osc))
  )

names(tb_JoinOSC)


RM_SP$cd_municipio <- as.character(RM_SP$cd_municipio)

tb_JoinOSC$dt_fechamento_osc[1:100]
tb_JoinOSC$uf[1:100]

tabela <- tb_JoinOSC %>% 
  mutate(Dummy_SP = ifelse(CodMunicIBGE == "3550308", 1, 0), 
         Dummy_RM_SP = ifelse(CodMunicIBGE %in% RM_SP$cd_municipio, 1, 0), 
         Dummy_ESP = ifelse(uf == "SP", 1, 0), 
         dt_fechamento_osc = ymd(dt_fechamento_osc)) %>% 
  dplyr::filter(!is.na(dt_fechamento_osc), 
                dt_fechamento_osc > ymd("2021-01-01")) %>% 
  group_by(nr_ano_fechamento_osc) %>% 
  summarise(TotalBR = n(), 
            ESP = sum(Dummy_ESP, na.rm = TRUE),
            RM_SP = sum(Dummy_RM_SP, na.rm = TRUE), 
            SP = sum(Dummy_SP, na.rm = TRUE))


# Data atualização:
max(ymd(tb_JoinOSC$data_situacao_cadastral), na.rm = TRUE)
# "2023-08-11"

tabela

fwrite(tabela, "data/temp/BDSP.csv")


# Fim #####