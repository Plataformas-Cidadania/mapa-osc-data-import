# Script para extrair data de fechamento das OSC

# usa dados da Receita Federal (base dos CNPJs) para extrair a "data da última situação"
# Para as OSC baixadas, a última atualização é sempre a data do fechamento

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-06-20

# Setup ####

library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)

# Fonte dos dados: 
FonteRFB <- "CNPJ/SRF/MF/2023_01"

# Banco de dados dos CNPJ de organizações sem fins lucrativos da RFB (julho 2023)
tb_JoinOSC <- readRDS("backup_files/2023_01/intermediate_files/tb_JoinOSC.RDS")

# base tb_osc do MOSC (extração em 2024-06-21)
tb_osc_atual <- readRDS("data/temp/2024-06-21 Extracao/tb_osc_atual.RDS")

# Extrai data de fechamento ####


names(tb_osc_atual)
names(tb_JoinOSC)

# Cria banco de dados que faz a relação entre o id da OSC e o CNPJ
# Como somente tem id os CNPJ que são OSC (há uma exceção abaixo), então
# podemos usar esse banco para selecionar apenas as OSC da base das receitas.
idControl <- tb_osc_atual %>% 
  # É necessário consertar o padding do CNPJ das OSC
  mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                        side = "left", width = 14, pad = "0")) %>% 
  select(id_osc, cd_identificador_osc, bo_osc_ativa)

# Extrai variável de fechamento das OSC
DtFechamentoOSC <- tb_JoinOSC %>% 
  rename(cd_identificador_osc = cnpj) %>% 
  left_join(idControl, by = "cd_identificador_osc") %>% 
  dplyr::filter(
    # Exclui CNPJs que não foram identificados como OSC
    !is.na(id_osc), 
    # Não faz sentido extrair data da situação das OSC ativas:
    !bo_osc_ativa) %>% 
  mutate(
    # Existem casos em que a data_situacao_cadastral é '0', mesmo em OSC inativas.
    # Investiguei todos esses casos e me parece que são falhas de versões anteriores 
    # do find_osc, pois não me parecem OSC pelos critérios adotados. Vou excluir essas
    # OSC da amostra (são 1704 casos).
    data_situacao_cadastral = ifelse(data_situacao_cadastral == 0, 
                                     NA, data_situacao_cadastral),
    dt_fechamento_osc = ymd(data_situacao_cadastral),
    # Ano de fechamento OSC
    nr_ano_fechamento_osc = year(dt_fechamento_osc),
    # coloca fonte dos dados:
    ft_fechamento_osc = FonteRFB,
    ft_ano_fechamento_osc = FonteRFB) %>% 
  # Seleciona apenas variáveis relevantes
  select(id_osc, dt_fechamento_osc, ft_fechamento_osc, nr_ano_fechamento_osc, 
         ft_ano_fechamento_osc)

# Algumas análises rápicas
# names(DtFechamentoOSC)
# table(DtFechamentoOSC$nr_ano_fechamento_osc, useNA = "always")

# Insere novas variáveis no banco tb_osc:

tb_osc_att <- tb_osc_atual %>% 
  left_join(DtFechamentoOSC, by = "id_osc")

names(tb_osc_att)

# Análise dos dados:
table(is.na(tb_osc_att$nr_ano_fechamento_osc))

tb_osc_att %>% 
  mutate(has_date = ifelse(!is.na(nr_ano_fechamento_osc), 
                           "Tem data fechamento", "Sem data fechamento")) %>% 
  group_by(has_date, bo_osc_ativa) %>% 
  summarise(f = n()) %>% 
  spread(has_date, f)


# Salva tabela com a nova variável
saveRDS(tb_osc_att, "data/raw/RFB/dt_fechamentoOSC.RDS")

rm(tb_osc_att, DtFechamentoOSC, tb_JoinOSC, tb_osc_atual)
rm(FonteRFB, idControl)

# Fim ####