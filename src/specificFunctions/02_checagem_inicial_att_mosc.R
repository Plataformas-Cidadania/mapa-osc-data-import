# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Antes de realizar a atualização propriamente dita, melhor fazer uma 
# série de checagens para saber se todos os dados e as tabelas auxiliares 
# estão disponíveis e se a conexão com o banco de dados está operacional e com
# as permissões necessárias.

# Autor do Script: Murilo Junqueira 
# m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12 
# (baseado em scripts anteriores a partir de 2023-09)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes 

## Funções auxiliares:
# "src/generalFunctions/postCon.R"
# "src/generalFunctions/agora.R"

## Outputs:
# (nenhum)

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Checagem Inicial da Atualização ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message(agora(), ": Checagem inicial de da atualiação...")
Sys.sleep(1)

# Verifica se o caminho para o arquivo de atualização está correto.
assert_that(file.exists(definicoes$path_rscript_att_mosc), 
            msg = "Atualize caminho do arquivo de atualização!") %>% 
  if(.) message("Caminho do arquivo de atualização correto")

Sys.sleep(2) # vou colocar uma pausa após cada mensagem para o usuário ter tempo de ler

# Verifica se o diretório de arquivo de backups existe,
assert_that(dir.exists(definicoes$dir_backup_files), 
            msg = "Diretório dos arquivos de backups da atualização não encontrado") %>% 
  if(.) message("Diretório de backup encontrado.")

# Aproveito para corrigir eventuais problemas de falta de "/":
if(str_sub(definicoes$dir_backup_files, -1, -1) != "/") {
  dir_backup_files <- paste0(dir_backup_files, "/")
}

Sys.sleep(2)


# Verifica se as tabelas auxiliares estão presentes no diretório 'tab_auxiliares'.

## Controle da variável id_osc, de acordo com a última atualização:
assert_that(file.exists("tab_auxiliares/idControl.RDS"),
            msg = glue("O arquivo de ID das OSC da última versão ", 
                       "não está disponível")) %>% 
  if(.) message("O arquivo de ID das OSC da última versão encontrado em",
                "'tab_auxiliares'")

Sys.sleep(2)

## Controle do ID da tabela tb_area_atuacao
assert_that(file.exists("tab_auxiliares/idAreaAtuacaoControl.RDS"),
            msg = glue("O arquivo de ID das área de atuação das OSC da ", 
                       "última versão não está disponível")) %>% 
  if(.) message("O arquivo de ID das área de atuação encontrado ", 
                "em 'tab_auxiliares'")

Sys.sleep(2)

## Tabela com os nomes que indicam que determinado CNPJ não é OSC
assert_that(file.exists("tab_auxiliares/NonOSCNames.csv"),
            msg = glue("O arquivo 'NonOSCNames.csv' contendo as expressões ", 
                       "regulares para diferenciar OSC de não OSC ", 
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'NonOSCNames.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)

## Relação entre áreas e subáreas de atuação
assert_that(file.exists("tab_auxiliares/Areas&Subareas.csv"),
            msg = glue("O arquivo 'Areas&Subareas.csv' contendo as a relação ", 
                       "entre áreas e subáreas de OSC ", 
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'Areas&Subareas.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)

## Critérios para determinar as áreas de atuação
assert_that(file.exists("tab_auxiliares/IndicadoresAreaAtuacaoOSC.csv"),
            msg = glue("O arquivo 'IndicadoresAreaAtuacaoOSC.csv' contendo ",
                       "as regras para determinar as áreas de atuação das OSC ",
                       "a partir dos dados da CNAE ",
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'IndicadoresAreaAtuacaoOSC.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)

## Veja se os descritórios dos códigos das áreas estão das tabelas auxiliares
assert_that(file.exists("tab_auxiliares/dc_area_atuacao.csv"),
            msg = glue("O arquivo 'dc_area_atuacao.csv' contendo os códigos ", 
                       "da área de atuação das OSC ", 
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'dc_area_atuacao.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)

## Veja se os descritórios dos códigos das áreas estão das tabelas auxiliares
assert_that(file.exists("tab_auxiliares/dc_subarea_atuacao.csv"),
            msg = glue("O arquivo 'dc_subarea_atuacao.csv' contendo os códigos ", 
                       "das subáreas de atuação das OSC ", 
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'dc_subarea_atuacao.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)

## Veja se o "de/para" do código municipal da receita para o IBGE está presente.
assert_that(file.exists("tab_auxiliares/CodMunicRFB.csv"),
            msg = glue("O arquivo 'CodMunicRFB.csv' contendo o de/para ", 
                       "dos códigos de município da Receita Federal e do IBGE ", 
                       "não foi encontrado em 'tab_auxiliares'")) %>% 
  if(.) message("Arquivo 'CodMunicRFB.csv' encontrado em 'tab_auxiliares'")

Sys.sleep(2)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Checagem da Conexão com o MOSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Função para facilitar a conexão com os bancos de dados PostgreSQL:
assert_that(file.exists("src/generalFunctions/postCon.R"))
source("src/generalFunctions/postCon.R") 
assert_that(is.function(postCon))

message(agora(), "  Concectando aos Bancos de Dados MOSC")
Sys.sleep(1) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Primeiro vamos checar se conseguimos nos conectar ao banco de dados:
assert_that("credenciais_mosc" %in% names(definicoes), 
            msg = "O objeto 'credenciais_mosc', não foi carregado!")

# Concecta aos bancos de dados do MOSC:
conexao_mosc <- postCon(definicoes$credenciais_mosc, 
                        Con_options = "-c search_path=osc")

if(dbIsValid(conexao_mosc)) message("Conectado ao BD 'portal_osc'")

# Testa extrair dados do banco
teste <- try(dbGetQuery(conexao_mosc, 
                        paste0("SELECT * FROM tb_osc",
                               " LIMIT 500", 
                               ";")))
assert_that(!is.error(teste), 
            msg = glue("Não foi possível ler uma amostra do banco ", 
                       "'portal_osc'
                       Teste de leitura 'portal_osc' falhou!
                       ")) %>% 
  if(.) message("Teste de leitura do banco 'portal_osc' realizado com ", 
                "sucesso")

Sys.sleep(2)

# Testa inserir uma nova tabela:
if(dbExistsTable(conexao_mosc, "teste")) try(dbRemoveTable(conexao_mosc, "teste"))
try(dbWriteTable(conexao_mosc, "teste", teste))

# Verifica se a tabela fo inserida corretamente
assert_that(dbExistsTable(conexao_mosc, "teste"), 
            msg = glue("Não foi possível inserir uma tabela no banco ", 
                       "'portal_osc'
                       Teste de inserção de tabela no 'portal_osc' falhou!
                       ")) %>% 
  if(.) message("Teste de inserção de tabela no 'portal_osc' realizado com ", 
                "sucesso")

Sys.sleep(2)

# Checa se a tabela inserida no banco está correta
teste_verific <- dbGetQuery(conexao_mosc, "SELECT * FROM teste;")

assert_that(all(names(teste) == names(teste_verific)) &&
              all(dim(teste) == dim(teste_verific)), 
            msg = glue("A tabela inserida no banco  'portal_osc' não está ", 
                       "igual a amostra.",
                       "
                       Teste de integridade de inserção no 'portal_osc' falhou!
                       ")) %>% 
  if(.) message("Teste de integridade de inserção no 'portal_osc' realizado ", 
                "com sucesso") 
# head(teste)
# head(teste_verific)
# tail(teste)
# tail(teste_verific)

Sys.sleep(2)

# Verifica se podemos deletar tabela inserida:
if(dbExistsTable(conexao_mosc, "teste")) {
  try(dbRemoveTable(conexao_mosc, "teste"))
}

assert_that(!dbExistsTable(conexao_mosc, "teste"), 
            msg = glue("Não foi possível deletar a tabela teste inserida em ",
                       "'portal_osc' não está ", 
                       "
                       Teste de exclusão de inserção no 'portal_osc' falhou!
                       ")) %>% 
  if(.) message("Teste de exclusão de inserção no 'portal_osc' realizado ", 
                "com sucesso") 

Sys.sleep(2)

rm(teste, teste_verific)


# Verifica se as tabelas de controle de backup estão presentes:
tables <- dbListTables(conexao_mosc)

teste_tabelas_controle <- c("tb_controle_atualizacao", "tb_backups_files", 
  "tb_processos_atualizacao") %in% tables

assert_that(all(teste_tabelas_controle), 
            msg = glue("Não foi possível encontrar a(s) tabela(s) de controle:  ",
                       paste0(c("tb_controle_atualizacao", "tb_backups_files", 
                                "tb_processos_atualizacao")[teste_tabelas_controle], 
                              collapse = ", "), ".", 
                       "
                       Verifique o erro ou execute novamente o script ", 
                       "'src/uso_unico/cria_controle_att.R'")) %>% 
  if(.) message("Tabelas de controle de atualização encontradas com ", 
                "com sucesso") 
rm(tables, teste_tabelas_controle)

Sys.sleep(2)

# Verifica PostGIS
testeFuncGeo <- try(
  dbGetQuery(conexao_mosc, 
             "SELECT public.ST_MakePoint(-50.3482090039996,-20.7619611619996);")
                    )

assert_that(!is.error(testeFuncGeo), 
            msg = glue("Função 'public.ST_MakePoint' não encontrada.
                       Provavelmente o PostGIS não está corretamente instalado.")) %>% 
  if(.) message("Teste do PostGIS no 'portal_osc' realizado ", 
                "com sucesso") 
rm(testeFuncGeo)
Sys.sleep(2)

# De forma profilática, desconecta do banco de dados
dbDisconnect(conexao_mosc)
rm(conexao_mosc)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Checagem da Conexão com o banco contendo os dados da RFB (rais_2019) ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message(agora(), "  Concectando aos Bancos de Dados RFB (rais_2019)")
Sys.sleep(1) # Dar um tempo apenas para o usuário ler as mensagens da atualização

assert_that("credenciais_rfb" %in% names(definicoes), 
            msg = "O objeto 'credenciais_rfb', não foi carregado!")

conexao_rfb <- postCon(definicoes$credenciais_rfb, 
                       Con_options = glue("-c search_path={definicoes$schema_receita}"))
if(dbIsValid(conexao_rfb)) message("Conectado ao BD 'rais_2019'")

# Testa extrair dados do banco
teste_rfb <- try(dbGetQuery(conexao_rfb, 
                        glue("SELECT * FROM {definicoes$tabela_empresas_rfb}",
                               " LIMIT 500", 
                               ";")))

assert_that(!is.error(teste_rfb), 
            msg = glue("Não foi possível ler uma amostra do banco ", 
                       "'{definicoes$schema_receita}'
                       Teste de leitura de '{definicoes$schema_receita}' falhou!
                       ")) %>% 
  if(.) message("Teste de leitura do banco da Receita Federal realizado com ", 
                "sucesso")

Sys.sleep(2)

# De forma profilática, desconecta do banco de dados
dbDisconnect(conexao_rfb)
rm(conexao_rfb)
rm(postCon)


# Fim ####