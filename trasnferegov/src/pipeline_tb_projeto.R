
library(odbc)
library(DBI)
library(data.table)
library(dotenv)
library(RPostgres)

#load_dot_env()

con_db_name <- Sys.getenv("CON_DBNAME")
con_db_host <- Sys.getenv("CON_DBHOST")
con_db_user <- Sys.getenv("CON_DBUSER")
con_db_pass <- Sys.getenv("CON_DBPASS")
con_db_port <- as.integer(Sys.getenv("CON_DBPORT"))

con2_db_name <- Sys.getenv("CON2_DBNAME")
con2_db_host <- Sys.getenv("CON2_DBHOST")
con2_db_user <- Sys.getenv("CON2_DBUSER")
con2_db_pass <- Sys.getenv("CON2_DBPASS")
con2_db_port <- as.integer(Sys.getenv("CON2_DBPORT"))

con <- dbConnect(RPostgres::Postgres(),
  dbname = con_db_name,
  host = con_db_host,
  user = con_db_user,
  password = con_db_pass,
  port = con_db_port
)

print(paste("Conexão 1:",dbIsValid(con)))

con2 <- dbConnect(RPostgres::Postgres(),
  dbname = con2_db_name,
  host = con2_db_host,
  user = con2_db_user,
  password = con2_db_pass,
  port = con2_db_port
)

print(paste("Conexão 2:",dbIsValid(con2)))

tb_projeto <- data.table(dbGetQuery(con, "SELECT * FROM osc.tb_projeto"))


query <- "
  INSERT INTO public.tb_projeto (
    id_projeto, id_osc, tx_nome_projeto, ft_nome_projeto, cd_status_projeto, ft_status_projeto,
    dt_data_inicio_projeto, ft_data_inicio_projeto, dt_data_fim_projeto, ft_data_fim_projeto,
    tx_link_projeto, ft_link_projeto, nr_total_beneficiarios, ft_total_beneficiarios, nr_valor_captado_projeto,
    ft_valor_captado_projeto, nr_valor_total_projeto, ft_valor_total_projeto, cd_abrangencia_projeto, ft_abrangencia_projeto,
    cd_zona_atuacao_projeto, ft_zona_atuacao_projeto, tx_descricao_projeto, ft_descricao_projeto,
    ft_metodologia_monitoramento, tx_metodologia_monitoramento, tx_identificador_projeto_externo, ft_identificador_projeto_externo,
    bo_oficial, tx_status_projeto_outro, cd_municipio, ft_municipio, cd_uf, ft_uf
  )
  VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34
  )
  ON CONFLICT(id_projeto) DO UPDATE SET
    id_projeto = excluded.id_projeto,
    id_osc = excluded.id_osc,
    tx_nome_projeto = excluded.tx_nome_projeto,
    ft_nome_projeto = excluded.ft_nome_projeto,
    cd_status_projeto = excluded.cd_status_projeto,
    ft_status_projeto = excluded.ft_status_projeto,
    dt_data_inicio_projeto = excluded.dt_data_inicio_projeto,
    ft_data_inicio_projeto = excluded.ft_data_inicio_projeto,
    dt_data_fim_projeto = excluded.dt_data_fim_projeto,
    ft_data_fim_projeto = excluded.ft_data_fim_projeto,
    tx_link_projeto = excluded.tx_link_projeto,
    ft_link_projeto = excluded.ft_link_projeto,
    nr_total_beneficiarios = excluded.nr_total_beneficiarios,
    ft_total_beneficiarios = excluded.ft_total_beneficiarios,
    nr_valor_captado_projeto = excluded.nr_valor_captado_projeto,
    ft_valor_captado_projeto = excluded.ft_valor_captado_projeto,
    nr_valor_total_projeto = excluded.nr_valor_total_projeto,
    ft_valor_total_projeto = excluded.ft_valor_total_projeto,
    cd_abrangencia_projeto = excluded.cd_abrangencia_projeto,
    ft_abrangencia_projeto = excluded.ft_abrangencia_projeto,
    cd_zona_atuacao_projeto = excluded.cd_zona_atuacao_projeto,
    ft_zona_atuacao_projeto = excluded.ft_zona_atuacao_projeto,
    tx_descricao_projeto = excluded.tx_descricao_projeto,
    ft_descricao_projeto = excluded.ft_descricao_projeto,
    ft_metodologia_monitoramento = excluded.ft_metodologia_monitoramento,
    tx_metodologia_monitoramento = excluded.tx_metodologia_monitoramento,
    tx_identificador_projeto_externo = excluded.tx_identificador_projeto_externo,
    ft_identificador_projeto_externo = excluded.ft_identificador_projeto_externo,
    bo_oficial = excluded.bo_oficial,
    tx_status_projeto_outro = excluded.tx_status_projeto_outro,
    cd_municipio = excluded.cd_municipio,
    ft_municipio = excluded.ft_municipio,
    cd_uf = excluded.cd_uf,
    ft_uf = excluded.ft_uf"

tempo_inicio <- Sys.time()
dbBegin(con2)

dbExecute(con2, query, params = unname(as.list(tb_projeto)))


dbCommit(con2)
tempo_fim <- Sys.time()
tempo_total <- tempo_fim - tempo_inicio
print(paste("Tempo de execução:", tempo_total, "segundos"))

