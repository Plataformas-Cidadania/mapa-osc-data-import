# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Formatar os dados enviados por fontes além da RFB (CEBAS, CNEAS, CNES etc)
# para determina a área de atuação das OSC (atualização 2024_01)

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-11-06


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# bibliotecas necessárias:
library(magrittr)
library(data.table)
library(dplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(readxl)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CEBAS Educação ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


cebas_edu_data <- read_xlsx("backup_files/2024_01/input_files/dados cebas educação.xlsx", 
                            sheet = "SITUAÇÃO 2023")

tb_area_atuacao_cebas_educacao <- cebas_edu_data %>% 
  rename(cd_identificador_osc = CNPJ) %>% 
  mutate(tx_area_atuacao = "Educação e pesquisa", 
         tx_subarea_atuacao = NA_character_, 
         ft_area_atuacao = "CEBAS Educação/MEC", 
         
         cd_identificador_osc = str_remove_all(cd_identificador_osc, "[:punct:]"), 
         
         cd_identificador_osc = str_pad(cd_identificador_osc, 
                                        width = 14,
                                        side = "left", 
                                        pad = "0")
  ) %>% 
  distinct(cd_identificador_osc, .keep_all = TRUE) %>% 
  select(cd_identificador_osc, tx_area_atuacao, 
         tx_subarea_atuacao, ft_area_atuacao)


fwrite(tb_area_atuacao_cebas_educacao, 
       "backup_files/2024_01/input_files/tb_area_atuacao_cebas_educacao.csv")

rm(tb_area_atuacao_cebas_educacao, cebas_edu_data)




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CEBAS Saúde ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


cebas_saude_data <- read_xlsx("backup_files/2024_01/input_files/dados_cebas_saude_2023_11_29.xlsx", 
                            sheet = "TB_ENTIDADE_CEBAS")

tb_area_atuacao_cebas_saude <- cebas_saude_data %>% 
  rename(cd_identificador_osc = NU_CNPJ) %>% 
  mutate(tx_area_atuacao = "Saúde", 
         tx_subarea_atuacao = NA_character_, 
         ft_area_atuacao = "CEBAS Saúde/MS", 
         
         cd_identificador_osc = str_remove_all(cd_identificador_osc, "[:punct:]"), 
         
         cd_identificador_osc = str_pad(cd_identificador_osc, 
                                        width = 14,
                                        side = "left", 
                                        pad = "0")
  ) %>% 
  distinct(cd_identificador_osc, .keep_all = TRUE) %>% 
  select(cd_identificador_osc, tx_area_atuacao, 
         tx_subarea_atuacao, ft_area_atuacao)


fwrite(tb_area_atuacao_cebas_saude, 
       "backup_files/2024_01/input_files/tb_area_atuacao_cebas_saude.csv")

rm(tb_area_atuacao_cebas_saude, cebas_saude_data)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CEBAS Assistência Social ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cebas_assistencia_data <- read_xlsx("backup_files/2024_01/input_files/dados_cebas_mds.xlsx", 
                                    sheet = "Sheet1")

names(cebas_assistencia_data)

tb_area_atuacao_cebas_mds <- cebas_assistencia_data %>% 
  rename(cd_identificador_osc = CNPJ) %>% 
  dplyr::filter(cd_identificador_osc != "-") %>% 
  mutate(tx_area_atuacao = "Assistência social", 
         tx_subarea_atuacao = NA_character_, 
         ft_area_atuacao = "CEBAS/MDS", 
         
         cd_identificador_osc = str_remove_all(cd_identificador_osc, "[:punct:]"), 
         
         cd_identificador_osc = str_pad(cd_identificador_osc, 
                                        width = 14,
                                        side = "left", 
                                        pad = "0")
  ) %>% 
  distinct(cd_identificador_osc, .keep_all = TRUE) %>% 
  select(cd_identificador_osc, tx_area_atuacao, 
         tx_subarea_atuacao, ft_area_atuacao)


fwrite(tb_area_atuacao_cebas_mds, 
       "backup_files/2024_01/input_files/tb_area_atuacao_cebas_mds.csv")

rm(tb_area_atuacao_cebas_mds, cebas_assistencia_data)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CNEAS ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


cneas_assistencia_data <- read_xlsx("backup_files/2024_01/input_files/dados_cneas_mds.xlsx", 
                                    sheet = "Sheet1")

names(cneas_assistencia_data)

tb_area_atuacao_cneas_mds <- cneas_assistencia_data %>% 
  
  rename(cd_identificador_osc = CNPJ_CNEAS) %>% 
  
  mutate(tx_area_atuacao = "Assistência social", 
         
         tx_subarea_atuacao = NA_character_, 
         
         ft_area_atuacao = "CEBAS/MDS", 
         
         cd_identificador_osc = str_remove_all(cd_identificador_osc, "[:punct:]"), 
         
         cd_identificador_osc = str_pad(cd_identificador_osc, 
                                        width = 14,
                                        side = "left", 
                                        pad = "0")
  ) %>% 
  
  distinct(cd_identificador_osc, .keep_all = TRUE) %>% 
  
  select(cd_identificador_osc, tx_area_atuacao, 
         tx_subarea_atuacao, ft_area_atuacao)


fwrite(tb_area_atuacao_cneas_mds, 
       "backup_files/2024_01/input_files/tb_area_atuacao_cneas_mds.csv")

rm(tb_area_atuacao_cneas_mds, cneas_assistencia_data)



# Fim ####