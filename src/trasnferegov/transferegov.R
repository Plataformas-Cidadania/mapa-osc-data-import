# Pacotes necessários
print("Verificando pacotes necessários...")
if (!requireNamespace("data.table", quietly = TRUE)) stop("Pacote 'data.table' não está instalado.")
if (!requireNamespace("readr", quietly = TRUE)) stop("Pacote 'readr' não está instalado.")
if (!requireNamespace("DBI", quietly = TRUE)) stop("Pacote 'DBI' não está instalado.")

library(data.table)
library(readr)
library(DBI)

# Carregar variáveis de ambiente do arquivo .env
print("Carregando variáveis de ambiente do arquivo .env...")
dotenv::load_dot_env()

# Criar pasta central
print("Criando pasta central 'dados_siconv'...")
if (!dir.exists("dados_siconv")) dir.create("dados_siconv")

# URLs e nomes dos arquivos
print("Definindo URLs e nomes dos arquivos...")
url_proposta <- "http://repositorio.dados.gov.br/seges/detru/siconv_proposta.csv.zip"
url_convenio <- "http://repositorio.dados.gov.br/seges/detru/siconv_convenio.csv.zip"
zip_proposta <- "dados_siconv/siconv_proposta.csv.zip"
zip_convenio <- "dados_siconv/siconv_convenio.csv.zip"

# Download dos arquivos
#print("Baixando arquivos...")
#download.file(url_proposta, destfile = zip_proposta, mode = "wb")
#download.file(url_convenio, destfile = zip_convenio, mode = "wb")
#
## Descompactar dentro da pasta central
#print("Descompactando arquivos...")
#unzip(zip_proposta, exdir = "dados_siconv")
#unzip(zip_convenio, exdir = "dados_siconv")

# Identificar os arquivos CSV
print("Identificando arquivos CSV...")
csvs <- list.files("dados_siconv", pattern = "\\.csv$", full.names = TRUE)
csv_proposta <- csvs[grepl("proposta", csvs)]
csv_convenio <- csvs[grepl("convenio", csvs)]

# Ler os dados com codificação Latin1 (UTF-8) e converter para data.table
print("Lendo dados dos arquivos CSV...")
dados_proposta <- fread(csv_proposta, encoding = "UTF-8", sep = ";")
dados_convenio <- fread(csv_convenio, encoding = "UTF-8", sep = ";")

# Mesclar os dados usando data.table
print("Mesclando dados...")
dados_mesclados <- merge(dados_proposta, dados_convenio, by = "ID_PROPOSTA", all = FALSE)

# Obter a data atual
print("Obtendo data atual...")
data_atual <- Sys.Date()

# Criar a variável fonte_carga
print("Criando variável 'fonte_carga'...")
dados_mesclados[, fonte_carga := paste0("SICONV/MPOG ", format(data_atual, "%d-%m-%Y"))]

# Criar a nova coluna 'link' com base no número do convênio
print("Criando coluna 'link'...")
dados_mesclados[, link := paste0(
  "https://www.convenios.gov.br/siconv/ConsultarProposta/ResultadoDaConsultaDeConvenioSelecionarConvenio.do?sequencialConvenio=",
  NR_CONVENIO,
  "&Usr=guest&Pwd=guest"
)]

# Criar a variável nome do projeto
print("Criando variável 'nome_projeto'...")
dados_mesclados[, nome_projeto := paste0("Parceria ", NR_CONVENIO)]

# Criar a nova coluna com base na lógica fornecida
print("Criando coluna 'cd_status_projeto'...")
dados_mesclados[, cd_status_projeto := fifelse(
  SIT_CONVENIO %in% c(
    "Proposta/Plano de Trabalho Aprovado",
    "Proposta/Plano de Trabalho Complementado Enviado para Análise"
  ), 3,
  fifelse(
    SIT_CONVENIO %in% c(
      "Em execução", "Assinado", "Assinado Pendente Registro TV Siafi"
    ), 4,
    fifelse(
      SIT_CONVENIO %in% c(
        "Inadimplente", "Aguardando Prestação de Contas", "Prestação de Contas Aprovada",
        "Prestação de Contas Aprovada com Ressalvas", "Prestação de Contas em Análise",
        "Prestação de Contas em Complementação", "Prestação de Contas enviada para Análise",
        "Prestação de Contas Iniciada Por Antecipação", "Prestação de Contas Rejeitada"
      ), 2,
      fifelse(
        SIT_CONVENIO %in% c("Cancelado", "Convênio Anulado"), 1, 4
      )
    )
  )
)]

# Substituir - por vazio na coluna CEP_PROPONENTE
print("Substituindo '-' por vazio na coluna 'CEP_PROPONENTE'...")
dados_mesclados[, CEP_PROPONENTE := gsub("-", "", CEP_PROPONENTE)]

# Substituir , por . nas colunas de valores monetários
print("Substituindo ',' por '.' nas colunas de valores monetários...")
colunas_valores <- c(
  "VL_CONTRAPARTIDA_CONV", "VL_CONTRAPARTIDA_PROP", "VL_DESEMBOLSADO_CONV",
  "VL_EMPENHADO_CONV", "VL_GLOBAL_CONV", "VL_GLOBAL_PROP",
  "VL_INGRESSO_CONTRAPARTIDA", "VL_REPASSE_CONV", "VL_REPASSE_PROP",
  "VL_SALDO_REMAN_CONVENENTE", "VL_SALDO_REMAN_TESOURO"
)

dados_mesclados[, (colunas_valores) := lapply(.SD, function(x) as.numeric(gsub(",", ".", x))), .SDcols = colunas_valores]

# Substituir aspas simples ' por aspas duplas " na coluna OBJETO_PROPOSTA
print("Substituindo aspas simples por aspas duplas na coluna 'OBJETO_PROPOSTA'...")
dados_mesclados[, OBJETO_PROPOSTA := gsub("'", "\"", OBJETO_PROPOSTA)]

# Salvar resultado
print("Preparando dados para inserção no banco de dados...")

# Gerar o comando SQL para inserção/atualização
print("Gerando comando SQL...")
query <- "
INSERT INTO osc.tb_projeto (
  cd_status_projeto, ft_status_projeto, tx_descricao_projeto, ft_descricao_projeto,
  dt_data_inicio_projeto, ft_data_inicio_projeto, dt_data_fim_projeto, ft_data_fim_projeto,
  nr_valor_total_projeto, nr_valor_captado_projeto, ft_valor_total_projeto, ft_valor_captado_projeto,
  bo_oficial, tx_nome_projeto, ft_nome_projeto, ft_identificador_projeto_externo,
  tx_link_projeto, ft_link_projeto, id_osc, tx_identificador_projeto_externo
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
)
ON CONFLICT (id_osc, tx_identificador_projeto_externo)
DO UPDATE SET
  cd_status_projeto = excluded.cd_status_projeto,
  ft_status_projeto = excluded.ft_status_projeto,
  tx_descricao_projeto = excluded.tx_descricao_projeto,
  ft_descricao_projeto = excluded.ft_descricao_projeto,
  dt_data_inicio_projeto = excluded.dt_data_inicio_projeto,
  ft_data_inicio_projeto = excluded.ft_data_inicio_projeto,
  dt_data_fim_projeto = excluded.dt_data_fim_projeto,
  ft_data_fim_projeto = excluded.ft_data_fim_projeto,
  nr_valor_total_projeto = excluded.nr_valor_total_projeto,
  nr_valor_captado_projeto = excluded.nr_valor_captado_projeto,
  ft_valor_total_projeto = excluded.ft_valor_total_projeto,
  ft_valor_captado_projeto = excluded.ft_valor_captado_projeto,
  bo_oficial = excluded.bo_oficial,
  tx_nome_projeto = excluded.tx_nome_projeto,
  ft_nome_projeto = excluded.ft_nome_projeto,
  ft_identificador_projeto_externo = excluded.ft_identificador_projeto_externo,
  tx_link_projeto = excluded.tx_link_projeto,
  ft_link_projeto = excluded.ft_link_projeto
"

# Preparar os dados para inserção/atualização
print("Preparando dados para inserção/atualização...")
dados_mesclados[, `:=`(
  cd_status_projeto = cd_status_projeto,
  ft_status_projeto = fonte_carga,
  tx_descricao_projeto = OBJETO_PROPOSTA,
  ft_descricao_projeto = fonte_carga,
  dt_data_inicio_projeto = DIA_INIC_VIGENC_CONV,
  ft_data_inicio_projeto = fonte_carga,
  dt_data_fim_projeto = DIA_FIM_VIGENC_CONV,
  ft_data_fim_projeto = fonte_carga,
  nr_valor_total_projeto = VL_REPASSE_CONV,
  nr_valor_captado_projeto = VL_DESEMBOLSADO_CONV,
  ft_valor_total_projeto = fonte_carga,
  ft_valor_captado_projeto = fonte_carga,
  bo_oficial = TRUE,
  tx_nome_projeto = nome_projeto,
  ft_nome_projeto = fonte_carga,
  ft_identificador_projeto_externo = fonte_carga,
  tx_link_projeto = link,
  ft_link_projeto = fonte_carga,
  id_osc = IDENTIF_PROPONENTE,
  tx_identificador_projeto_externo = NR_CONVENIO
)]

# Conectar ao banco de dados
print("Conectando ao banco de dados...")
con <- dbConnect(RPostgres::Postgres(),
  dbname = Sys.getenv("CON2_DBNAME"),
  host = Sys.getenv("CON2_DBHOST"),
  user = Sys.getenv("CON2_DBUSER"),
  password = Sys.getenv("CON2_DBPASS"),
  port = as.integer(Sys.getenv("CON2_DBPORT"))
)

# Verificar conexão
print("Verificando conexão com o banco de dados...")
print(paste("Connection 1:", dbIsValid(con)))


# Inserir/atualizar os dados em lotes
print("Inserindo/atualizando dados em lotes...")
batch_size <- 1000
n <- nrow(dados_mesclados)

for (i in seq(1, n, by = batch_size)) {
  print(paste("Processando lote", i, "de", n, "..."))
  batch <- dados_mesclados[i:min(i + batch_size - 1, n)]

  for (j in 1:nrow(batch)) {
    linha <- batch[j, , drop = FALSE]
    valores <- unname(as.list(linha[1, ]))
    dbExecute(con, query, params = valores)
  }
}



# Fechar a conexão
print("Fechando conexão com o banco de dados...")
dbDisconnect(con)