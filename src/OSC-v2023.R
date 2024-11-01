# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: atualizar as informações do Mapa das Organizações da 
# Sociedade Civil (MOSC), utilizando sobre tudo dados da Receita Federal 
# (base dos CNPJs) e da RAIS (MTE). Esse é um script mestre que controla 
# várias partes da atualização, que foram transformadas em funções e rotinas 
# específicas e colocadas em arquivos separados discriminados abaixo. 

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2023-10-19

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Definições importantes ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Nessa seção fazemos a definições de alguns parâmetros importantes que
# vão controlar toda a atualização

# para não poluir o ambiente de trabalho do R, colocarei todas as definições
# importantes em uma lista:
definicoes <- list()

#~~~~~~~~~~~~ MUITO IMPORTANTE: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Schema da Receita Federal de onde iremos retirar os dados:
definicoes$schema_receita <- "rfb_2024" # ATUALIZAR AQUI QUANDO CHEGAR NOVOS DADOS

# Colocar aqui a data de referência do dos dados originais (Receita Federal)
definicoes$data_dados_referencia <- ymd("2024-04-13") # ATUALIZAR AQUI TAMBÉM
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Arquivo JSON com as chave de acesso ao banco de dados MOSC
# definicoes$credenciais_mosc <- "keys/localhost_key.json"
definicoes$credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"

# Arquivo JSON com as chaves de acesso ao banco de dados da RFB e da RAIS:
definicoes$credenciais_rfb <- "keys/rais_2019_MuriloJunqueira.json"

# Essa atualização é teste? 
# (não registra processos novos nos controles)
definicoes$att_teste <- FALSE

# Essa atualização vai salvar os arquivos intermediários no diretório de backup?
definicoes$salva_backup <- TRUE

# Outras definições importantes, mas que raramente precisam ser mudada
# estão na rotina de Setup (abaixo) ('src/specificFunctions/setup_atualizacao.R')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualização MOSC: Leitura e Transformação dos Dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Carrega bibliotecas e funções usadas em toda a atualização:
source("src/specificFunctions/01_setup_atualizacao.R")

# Antes de realizar a atualização propriamente dita, melhor fazer uma 
# série de checagens para saber se todos os dados e as tabelas auxiliares 
# estão disponíveis e se a conexão com o banco de dados está operacional e com
# as permissões necessárias.
source("src/specificFunctions/02_checagem_inicial_att_mosc.R")

# Cria ou resgata (caso já criada) os controle de atualização, registrando data 
# e horário de cada etapa da atualização. Se os bancos de dados de controle de 
# atualizações não estiverem sido criados, utilizar a o 
# script "src/uso_unico/cria_controle_att.R"
source("src/specificFunctions/03_inicia_controle_atualizacao.R")

# Se não existir um diretório para guardar os arquivos da atualização, criar um...
source("src/specificFunctions/04_cria_diretorio_atualizacao.R")

# Baixa os dados brutos da Receita Federal
source("src/specificFunctions/05_baixa_dados_rfb.R")

# Identificação OSC na ba da Receita Federal (usando a função 'findosc-v2023')
source("src/specificFunctions/06_identificacao_osc_nabase_rfb.R")

# Determinação das áreas de atuação OSC
source("src/specificFunctions/07_determinacao_areas_atuacao.R")

# Estou aqui!!!! ####

# Gera o arquivo de INPUT de Geolocalização:
# (extrai as informações de endereços das OSC para que o software de localização
# como o GALILEO ou outro, possa gerar a latitude e longitude)
source("src/specificFunctions/08_gera_geolocalizacao.R")

# Neste ponto aqui o arquivo "intermediate_files/LatLonOSC.RDS" deve estar
# presente e atualizado:
Confirmation <- readline("O arquivo de geolocalização está atualizado? (s/n) ")
assert_that(Confirmation == "s")
rm(Confirmation)

# Desmembramento da base RFB
source("src/specificFunctions/09_desmembramento_base_rfb.R")

# Insere Dados da RAIS
source("src/specificFunctions/10_insere_dados_rais.R")


# Limpa memória
rm(ControleAtualizacao, Att_Atual)
rm(ProcessosAtualizacao, ProcessosAtt_Atual, BackupsFiles)
rm(DirName)
ls()

# Fim ####