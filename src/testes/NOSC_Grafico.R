# Script para criar um gráfico para mostrar abertura, fechamento e estoque de
# OSC 

# Usa dado de fechamento das OSC do script ExtraiFechamantoOSC.R

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-06-20


library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)


dt_fechamentoOSC <- readRDS("data/temp/dt_fechamentoOSC.RDS")
tb_dados_gerais <- readRDS("data/temp/tb_dados_gerais_atual.RDS")
tb_osc <- readRDS("data/temp/tb_osc_atual.RDS")


names(dt_fechamentoOSC)
names(tb_dados_gerais)
names(tb_osc)

tb_osc$cd_identificador_osc[1:1000] %>% as.character()


table(dt_fechamentoOSC$nr_ano_fechamento_osc)

names(NOSC)


str_sub("01988790000128", 9, 12)

class(tb_dados_gerais$dt_fundacao_osc)
tb_dados_gerais$dt_fundacao_osc[1:100]

NOSC <- tb_dados_gerais %>% 
  left_join(tb_osc, by = "id_osc") %>% 
  select(-bo_osc_ativa) %>% 
  left_join(select(dt_fechamentoOSC, 
                   id_osc, nr_ano_fechamento_osc, bo_osc_ativa), 
            by = "id_osc") %>% 
  mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                        width = 14, 
                                        side = "left", 
                                        pad = "0"), 
         Eh_matriz = ifelse(str_sub(cd_identificador_osc, 9, 12) == "0001", 1, 0), 
         nr_ano_fundacao_osc = year(ymd(dt_fundacao_osc))) %>%
  # Excluir casos estranhos (OSC fechadas e sem data de fechamento, cerca de 4.4k)
  dplyr::filter(!(!bo_osc_ativa & is.na(nr_ano_fechamento_osc)), 
                # Vou deletar casos sem data de fundação (681 casos)
                !is.na(nr_ano_fundacao_osc))

names(NOSC)
table(NOSC$nr_ano_fechamento_osc, useNA = "always")
table(NOSC$Eh_matriz, useNA = "always")

teste <- NOSC %>% 
  dplyr::filter(is.na(nr_ano_fundacao_osc))

table(teste$nr_ano_fechamento_osc, useNA = "always")

table(NOSC$nr_ano_fundacao_osc, useNA = "always")


plot_data <- tibble(Ano = 2010:2022) %>% 
  mutate(N_OSC = NA_integer_,
         N_OSCMatriz = NA_integer_, 
         N_OSCFiliais = NA_integer_, 
         Aberturas = NA_integer_, 
         Fechamentos = NA_integer_, 
         AberturasMatriz = NA_integer_, 
         FechamentosMatriz = NA_integer_, 
         AberturasFilial = NA_integer_, 
         FechamentosFilial = NA_integer_)

for (i in seq_along(plot_data$Ano)) {
  # i <- 1
  message(plot_data$Ano[i])
  
  # Soma abertura de OSC
  plot_data$Aberturas[i] <- sum(NOSC$nr_ano_fundacao_osc == plot_data$Ano[i], 
                                na.rm = TRUE)
  
  # Soma Fechamento de OSC
  plot_data$Fechamentos[i] <- sum(NOSC$nr_ano_fechamento_osc == plot_data$Ano[i], 
                                  na.rm = TRUE)
  # Agora somente para Matriz
  # Soma abertura de OSC
  plot_data$AberturasMatriz[i] <- sum(
    (NOSC$nr_ano_fundacao_osc == plot_data$Ano[i]) & (NOSC$Eh_matriz == 1), 
                                na.rm = TRUE)
  
  # Soma Fechamento de OSC
  plot_data$FechamentosMatriz[i] <- sum(
    (NOSC$nr_ano_fechamento_osc == plot_data$Ano[i]) & (NOSC$Eh_matriz == 1), 
                                  na.rm = TRUE)
  
  # Agora somente para Filiais
  # Soma abertura de OSC
  plot_data$AberturasFilial[i] <- sum(
    (NOSC$nr_ano_fundacao_osc == plot_data$Ano[i]) & (NOSC$Eh_matriz == 0), 
    na.rm = TRUE)
  
  # Soma Fechamento de OSC
  plot_data$FechamentosFilial[i] <- sum(
    (NOSC$nr_ano_fechamento_osc == plot_data$Ano[i]) & (NOSC$Eh_matriz == 0), 
    na.rm = TRUE)
  
  
  # Calcula o estoque de OSC (N de OSC)
  plot_data$N_OSC[i] <- 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          NOSC$nr_ano_fechamento_osc >= plot_data$Ano[i], 
        na.rm = TRUE) + 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          is.na(NOSC$nr_ano_fechamento_osc), 
        na.rm = TRUE)
  
  plot_data$N_OSCMatriz[i] <- 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          NOSC$nr_ano_fechamento_osc >= plot_data$Ano[i] & 
          NOSC$Eh_matriz == 1, 
        na.rm = TRUE) + 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          is.na(NOSC$nr_ano_fechamento_osc) & 
          NOSC$Eh_matriz == 1, 
        na.rm = TRUE)
  
  plot_data$N_OSCFiliais[i] <- 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          NOSC$nr_ano_fechamento_osc >= plot_data$Ano[i] & 
          NOSC$Eh_matriz == 0, 
        na.rm = TRUE) + 
    sum(NOSC$nr_ano_fundacao_osc <= plot_data$Ano[i] & 
          is.na(NOSC$nr_ano_fechamento_osc) & 
          NOSC$Eh_matriz == 0, 
        na.rm = TRUE)
  
  }


# Gráficos de Estoque OSC:

plot_data %>% 
  ggplot() + 
  geom_line(aes(x = Ano, y = N_OSCMatriz), color="red")+
  geom_line(aes(x = Ano, y = N_OSCFiliais), color="blue")+
  theme_classic() + 
  ylab("Número de OSC") +
  scale_x_continuous(breaks = as.integer(2010:2022)) + 
  scale_y_continuous(breaks = seq(0, 900000, by = 100000),
    labels = function(x) format(x, big.mark = ".",
                                                 scientific = FALSE)) + 
  theme(axis.text.x = element_text(angle = 45,hjust = 1))


plot_data %>% 
  select(Ano, N_OSCMatriz, N_OSCFiliais) %>% 
  rename(Matriz = N_OSCMatriz, 
         Filiais = N_OSCFiliais) %>% 
  gather(Tipo, Valor, Matriz:Filiais) %>% 
  ggplot(aes(x = Ano, y = Valor, group = Tipo, color = Tipo)) + 
  geom_line()+
  theme_classic() + 
  ylab("Número de OSC") +
  scale_x_continuous(breaks = as.integer(2010:2022)) + 
  scale_y_continuous(breaks = seq(0, 900000, by = 100000),
                     labels = function(x) format(x, big.mark = ".",
                                                 scientific = FALSE)) + 
  theme(axis.text.x = element_text(angle = 45,hjust = 1))



# 
plot_data %>% 
  ggplot(aes(x = Ano, y = N_OSC)) + 
  theme_classic() + 
  geom_line(color="red")+
  geom_point() + 
  ylab("Número de OSC") +
  scale_x_continuous(breaks = as.integer(2010:2022)) + 
  scale_y_continuous(breaks = seq(600000, 900000, by = 50000),
                     labels = function(x) format(x, big.mark = ".",
                                                 scientific = FALSE)) + 
  theme(axis.text.x = element_text(angle = 45,hjust = 1))



fwrite(plot_data, "data/temp/nosc.csv", 
       sep = ";", dec = ",")
