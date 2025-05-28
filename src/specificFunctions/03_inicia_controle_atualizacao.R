# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: cria linhas de controle da atualização na 
# tabela tb_controle_atualizacao. Cria uma conexão dbplyr com as tabelas
# tb_processos_atualizacao e tb_backups_files. Se houver uma atualização
# iniciada e não concluída, resgara os dados delas para sabermos de
# onde ela parou.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

## Inputs:
# definicoes 
# conexao_mosc (caso definicoes$att_teste == FALSE)

## Funções auxiliares:
# "src/specificFunctions/conexao_banco_mosc.R" (se necessário)
# "src/generalFunctions/agora.R"

## Outputs:
# processos_att_atual
# tb_controle_atualizacao
# tb_processos_atualizacao
# id_presente_att
# codigo_presente_att
# diretorio_att

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Controle das Atualizações ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Nesta parte iremos registrar o horário de cada etapa da atualização no próprio
# banco de dados MOSC, bem como os arquivos intermediários gerados.

message(agora(), ": Carregando controle de atualizações...")
Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Inicia a conexão com o bd portal_osc (se necessário):
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))){
  source("src/specificFunctions/conexao_banco_mosc.R")
}

# Objeto de tabela conectada ao banco de dados (dbplry)
tb_controle_atualizacao <- tbl(conexao_mosc, "tb_controle_atualizacao")

# Emite erro se houver duas atualizações iniciadas:
assert_that(sum(
  pull(tb_controle_atualizacao, tx_att_situacao) == "Iniciada") <= 1, 
  msg = "Há registro de duas atualizações iniciadas. Verifique o erro!")

# Caso não houver atualização iniciada, criar uma linha de atualização:
# (caso houver atualização iniciada, pegar os dados dela para continuar de onde
# se parou):

if(!any(pull(tb_controle_atualizacao, tx_att_situacao) == "Iniciada")) {
  
  # Cria linha de atualização (para uma atualização nova):
  
  # Determina o ID da atualização:
  id_presente_att <- ifelse(length(pull(tb_controle_atualizacao, att_id)) == 0, 
                            1, 
                            max(pull(tb_controle_atualizacao, att_id), 
                                na.rm = TRUE) + 1)
  
  # Código de referência da atualização (ano_<número da atualiação no ano>):
  codigo_presente_att <- 
    # começa calculando <número da atualiação no ano> 
    tb_controle_atualizacao %>% 
    dplyr::filter(dt_att_inicio > !!ymd(glue("{year(now())}-01-01"))) %>% 
    collect() %>% 
    nrow() %>% 
    magrittr::add(1) %>% 
    str_pad(width = 2, pad = 0) %>% 
    # Coloca aqui o ano da atualização (o presente ano):
    paste0(year(now()), "_", .)
  
  # Insere linha de atualização nova:
  rows_append(
    tb_controle_atualizacao, 
    copy_inline(conexao_mosc, 
                tibble(att_id = id_presente_att, 
                       cd_att_ref = codigo_presente_att, 
                       tx_att_situacao = "Iniciada", 
                       dt_att_ref = definicoes$data_dados_referencia, 
                       dt_att_inicio = lubridate::now(), 
                       dt_att_fim = NA_Date_,
                       .rows = 1)), 
    in_place = TRUE)
  
} else {
  
  # Aqui se pega os dados da atualização iniciada, de modo a se continuar ela:
  
  # Resgata ID da atualização atual:
  id_presente_att <- tb_controle_atualizacao %>% 
    dplyr::filter(tx_att_situacao == "Iniciada") %>% 
    select(att_id) %>% 
    collect() %>% 
    unlist() %>% as.integer()
  
  # Resgata código de referência da atualização atual:
  codigo_presente_att <- tb_controle_atualizacao %>% 
    dplyr::filter(att_id == id_presente_att) %>% 
    select(cd_att_ref) %>% 
    collect() %>% 
    unlist() %>% as.character()
  
}

# Controle dos processo da atualização atual:
tb_processos_atualizacao <- tbl(conexao_mosc, "tb_processos_atualizacao")

# Cria um array para mostrar quais processos foram executados na atualização atual:
processos_att_atual <- tb_processos_atualizacao %>% 
  dplyr::filter(att_id == id_presente_att) %>% 
  select(nr_processo_att_controle) %>% 
  collect() %>% unlist() %>% as.integer()

# Diretório da atualização dos dados:
diretorio_att <- paste0(definicoes$dir_backup_files,
                        codigo_presente_att, "/")

# Carrega tabela dos arquivos de backup:
tb_backups_files <- tbl(conexao_mosc, "tb_backups_files")

# Insere o comentário da Atualização
query_AltRow <- glue("UPDATE tb_controle_atualizacao \n",
                     " SET tx_att_comentarios = '{definicoes$tx_att_comentarios}' \n",
                     " WHERE att_id = {id_presente_att}",
                     ";")

# query_AltRow

dbExecute(conexao_mosc, query_AltRow)

rm(query_AltRow)

message(agora(), ": Controle de Atualização Carregado")
Sys.sleep(1) # Dar um tempo apenas para o usuário ler as mensagens da atualização

# Fim ####
