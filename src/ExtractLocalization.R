library(readxl)

# Dados do Galileo:

# Galileo_Raw <- read_excel("data/raw/Galileo/ExtractGalileo.xlsx", sheet = 1)
Galileo_Raw <- readRDS("data/raw/Galileo/ExtractGalileo.RDS")
OSC_Suplementar <- read_excel("data/raw/Galileo/osc_2024_04_09.xlsx", sheet = 1)

# names(OSC_Suplementar)
# class(Galileo_Raw$cd_identificador_osc)
# class(OSC_Suplementar$cd_identificador_osc)

OSC_Suplementar <- OSC_Suplementar %>% 
  mutate(New = !(cd_identificador_osc %in% Galileo_Raw$cd_identificador_osc), 
         tx_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                        width = 14, 
                                        side = "left", 
                                        pad = "0")) %>% 
  select(id_osc, tx_identificador_osc, 
         Longitude, Latitude, PrecisionDepth)

# names(OSC_Suplementar)

Galileo <- Galileo_Raw %>% 
  mutate(Flag = tx_identificador_osc %in% OSC_Suplementar[["tx_identificador_osc"]], 
         tx_identificador_osc = str_pad(as.character(tx_identificador_osc), 
                                        width = 14, 
                                        side = "left", 
                                        pad = "0")) %>% 
  dplyr::filter(!Flag) %>%
  select(id_osc, tx_identificador_osc, 
         Longitude, Latitude, PrecisionDepth) %>% 
  bind_rows(OSC_Suplementar) %>% 
  dplyr::filter(!is.na(PrecisionDepth),
                Longitude != 0, 
                Latitude != 0, 
                !is.na(Longitude), 
                !is.na(Latitude)) %>% 
  #group_by(tx_identificador_osc) %>% 
  #slice(1) %>% 
  #ungroup() %>% 
  mutate(PrecisionDepth = str_remove(PrecisionDepth, "Estrelas"), 
         PrecisionDepth = str_remove(PrecisionDepth, "Estrela"), 
         PrecisionDepth = str_trim(PrecisionDepth),
         PrecisionDepth = as.integer(PrecisionDepth)) %>% 
  rename(cd_identificador_osc = tx_identificador_osc, 
         cd_precisao_localizacao = PrecisionDepth)

sum(is.na(Galileo$PrecisionDepth))

saveRDS(Galileo, "data/raw/Galileo/GalileoINPUT.RDS")


rm(Galileo_Raw, Galileo)
