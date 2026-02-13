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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~ MUITO IMPORTANTE: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Schema da Receita Federal de onde iremos retirar os dados:
definicoes$schema_receita <- "rfb_unificado" # ATUALIZAR AQUI QUANDO CHEGAR NOVOS DADOS

# Escolhe o banco que vai ser atualizado 
# (escolher entre homologação e produção)
# As chaves deve estar em: "src/specificFunctions/01_setup_atualizacao.R"
# Opções: 'Produção' | 'Homologação'
definicoes$Banco_Atualização <- "Homologação" 

# Arquivo JSON com as chaves de acesso ao banco de dados da RFB e da RAIS:
definicoes$credenciais_rfb <- "keys/rais_2019_MuriloJunqueira.json"

# Adiciona um comentário para a atualização
definicoes$tx_att_comentarios <- glue::glue(
  "Atualição dos dados da RFB com base em extração em janeiro de 2026"
  )

# Essa atualização é teste? 
# (não registra processos novos nos controles)
definicoes$att_teste <- FALSE

# Essa atualização vai salvar os arquivos intermediários no diretório de backup?
definicoes$salva_backup <- TRUE

# Existem novos dados RAIS nesta atualização?
definicoes$atualiza_RAIS <- FALSE

definicoes$schemas_RAIS <- "vinculos_v6"
definicoes$tabela_RAIS <- "tb_vinculos"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

# Gera o arquivo de INPUT de Geolocalização:
source("src/specificFunctions/08_gera_geolocalizacao.R")

# Desmembramento da base RFB
source("src/specificFunctions/09_desmembramento_base_rfb.R")

# Insere Dados da RAIS
source("src/specificFunctions/10_insere_dados_rais.R")

# Insere Dados dos certificados
# source("src/specificFunctions/10b_atualiza_certificados.R")

# Insere Dados do quadro societário
source("src/specificFunctions/10c_insere_quadro_societario.R")


# Insere Dados do quadro societário
source("src/specificFunctions/10d_insere_transfere_gov.R")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Atualização MOSC: Update dos dados no Banco de Dados MOSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

source("src/specificFunctions/11_atualiza_mosc.R")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Finaliza a Rotina ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Extrai uma cópia em CSV para download no site
source("src/specificFunctions/12_resumo_base.R")

# Procedimentos finais de atualização:
source("src/specificFunctions/13_finaliza_att.R")


dbDisconnect(conexao_mosc)

# Limpa memória
rm(conexao_mosc, agora, atualiza_processos_att)
rm(definicoes, diretorio_att, codigo_presente_att, id_presente_att)
rm(processos_att_atual, tb_backups_files, tb_controle_atualizacao, 
   tb_processos_atualizacao)
ls()
gc()
# Fim ####
