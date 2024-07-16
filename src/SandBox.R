


library(glue)
library(lubridate)

glue::backtick()

tibble::tibble(x = "12/01/2022 12:00:00") |> 
  dplyr::mutate(y = lubridate::dmy_hms("12/01/2022 12:00:00"))

lubridate::dmy_hms(as.character(lubridate::now()))

message_glue <- function(x) {message(str_glue(x))}



for (i in seq_len(5)) {
  message_glue("Esse é o loop {i^2}: {format(now(), '%Y-%m-%d %H:%M:%S')}")
  Sys.sleep(1)
}


# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = keys$dbname,
                    host = keys$host,
                    port = keys$port,
                    user = keys$username, 
                    password = keys$password,
                    options="-c search_path=rfb_2023")

tb_JoinOSC <- dbGetQuery(connec, 
                         paste0("SELECT * FROM tb_rfb_empresas", 
                                " RIGHT JOIN tb_rfb_estabelecimentos ", 
                                "ON tb_rfb_estabelecimentos.cnpj_basico ",
                                "= tb_rfb_empresas.cnpj_basico", 
                                " WHERE natureza_juridica ",
                                "IN ('3069', '3220', '3301', '3999')",
                                # " LIMIT 1000", 
                                ";"))




"credenciais_mosc" %in% names(definicoes)



# Conexão aos bancos de dados do Mapa das Organizações da 
# Sociedade Civil (MOSC), o banco que queremos atualizar, mais o banco da Receita
# Federal (RFB) e da RAIS, de onde extraimos os dados:
source("src/specificFunctions/conexao_bancos_mosc_rfb.R")


as_datetime(now())


tabela_empresas_rfb <- definicoes$tabela_empresas_rfb
tabela_estabelecimentos_rfb <- definicoes$tabela_empresas_rfb
campo_cnpj <- definicoes$tabela_empresas_rfb
campo_natureza_juridica <- definicoes$tabela_empresas_rfb

glue(
  "SELECT * FROM {tabela_empresas_rfb} 
    RIGHT JOIN {tabela_estabelecimentos_rfb} 
    ON {definicoes$tabela_estabelecimentos_rfb}.{campo_cnpj} \\
    = {definicoes$tabela_empresas_rfb}.{campo_cnpj} 
    WHERE {campo_natureza_juridica} IN ('",
  paste(definicoes$natjur_nao_lucarivo, collapse = "', '"),
  "')",
  # " LIMIT 1000", 
  ";")


nested_func <- function(x, y) {
  nes_func2 <- function(x, y) x + y
  return(nes_func2(x, y))
}

nested_func(5, 8)
rm(nested_func)


dbExecute(conexao_mosc, 
          glue("DELETE FROM tb_processos_atualizacao 
                   WHERE processo_id = 9;"))

tb_processos_atualizacao

tables <- dbListTables(conexao_mosc)

tb_JoinOSC <- tb_JoinOSC %>% slice(1:100000)


teste <- tb_processos_atualizacao %>% collect()

# FALSE    TRUE 
# 28272 1609479

