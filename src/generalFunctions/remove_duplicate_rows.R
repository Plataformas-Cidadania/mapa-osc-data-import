# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: Remove linhas que estão totalmente duplicadas, 
# com exceção da chave primária.

# O script precisa ser adaptado a cada uso (nome da tabela e pk)

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2026-05-19

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Carregando os pacotes necessários
library(magrittr)
library(dplyr)
library(dbplyr)
library(DBI)
library(RPostgres)
library(glue)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conexão ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Inicia a conexão com o bd portal_osc (se necessário):
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))){
  source("src/specificFunctions/conexao_banco_mosc.R")
}



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Remove linhas duplicadas ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Definições da sua estrutura
con <- conexao_mosc # Conexão com o banco de dados
nome_tabela <- "tb_quadro_societario"
col_pk      <- "id_quadro_societario"

# 2. Criando a referência da tabela (processamento in-database)
tabela_db <- tbl(con, nome_tabela)

# 3. Identificando os IDs que serão deletados
# Agrupamos por todas as colunas exceto a Primary Key. 
# Nas linhas duplicadas, manteremos a que tiver o menor ID e deletaremos o resto.
ids_para_deletar <- tabela_db %>%
  group_by(across(-all_of(col_pk))) %>%
  dplyr::filter(n() > 1) %>%
  dplyr::filter(.data[[col_pk]] != min(.data[[col_pk]], na.rm = TRUE)) %>%
  ungroup() %>%
  select(all_of(col_pk)) %>%
  collect() %>% # Só agora trazemos o resultado (apenas os IDs) para a memória do R
  pull(1)

# length(ids_para_deletar)

# 4. Executando a deleção no banco de dados
if (length(ids_para_deletar) > 0) {
  
  # Usamos glue_sql para criar uma query parametrizada e blindada contra SQL Injection
  query_delete <- glue_sql(
    "DELETE FROM {`nome_tabela`} WHERE {`col_pk`} IN ({ids*})",
    ids = ids_para_deletar,
    .con = con
  )
  
  # Executamos o comando e capturamos o número de linhas afetadas
  linhas_removidas <- dbExecute(con, query_delete)
  
  message(glue::glue("Sucesso: {linhas_removidas} linhas duplicadas foram removidas e a integridade foi restaurada."))
  
} else {
  message("Excelente: Nenhuma linha duplicada foi encontrada no banco.")
}

# 5. Encerrando a conexão de forma limpa
dbDisconnect(con)


# Fim ####