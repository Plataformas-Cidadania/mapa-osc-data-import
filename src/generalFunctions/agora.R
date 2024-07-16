# Função facilitar o uso da marcação do tempo, formantando a função
# lubridate::now() para um formato mais amibável

# By Murilo Junqueira (m.junqueira@yahoo.com.br)
# Created at 2024-07-12

# Setup ####
library(lubridate)

# Debug: ####
# formato = '%Y-%m-%d %H:%M:%S'

# Function ####
agora <- function(formato = '%Y-%m-%d %H:%M:%S') {
  
  format(lubridate::now(), formato)
  
}
# rm(formato)

# Fim ####