# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: conectar aos bancos de dados do Mapa das Organizações da 
# Sociedade Civil (MOSC), o banco que queremos atualizar, mais o banco da Receita
# Federal (RFB) e da RAIS, de onde extraimos os dados.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conexão com os bancos de dados ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

assert_that(exists("credenciais_mosc"), 
            msg = "O objeto 'credenciais_mosc', não foi carregado!")

assert_that(exists("credenciais_rfb"), 
            msg = "O objeto 'credenciais_rfb', não foi carregado!")

# Função para facilitar a conexão com os bancos de dados PostgreSQL:
assert_that(file.exists("src/generalFunctions/postCon.R"))
source("src/generalFunctions/postCon.R") 
assert_that(is.function(postCon))

message(agora(), "  Concectando aos Bancos de Dados MOSC")
Sys.sleep(1) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Concecta aos bancos de dados do MOSC:
conexao_mosc <- postCon(credenciais_mosc, Con_options = "-c search_path=osc")
if(dbIsValid(conexao_mosc)) message("Conectado ao BD 'portal_osc'")

# Concecta aos bancos de dados da Receita:
message(agora(), "  Concectando aos Bancos de Dados RFB...")
Sys.sleep(1) 

conexao_rfb <- postCon(credenciais_rfb, 
                       Con_options = glue("-c search_path={schema_receita}"))
if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")

rm(postCon, credenciais_mosc, credenciais_rfb)

# Fim ####