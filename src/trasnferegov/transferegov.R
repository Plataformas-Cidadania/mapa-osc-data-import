# Pacotes necessários
if (!require("dplyr")) install.packages("dplyr", dependencies = TRUE)
if (!require("readr")) install.packages("readr")

library(dplyr)
library(readr)

# Criar pasta central
dir.create("dados_siconv", showWarnings = FALSE)

# URLs e nomes dos arquivos
url_proposta <- "http://repositorio.dados.gov.br/seges/detru/siconv_proposta.csv.zip"
url_convenio <- "http://repositorio.dados.gov.br/seges/detru/siconv_convenio.csv.zip"
zip_proposta <- "dados_siconv/siconv_proposta.csv.zip"
zip_convenio <- "dados_siconv/siconv_convenio.csv.zip"

# Download dos arquivos
download.file(url_proposta, destfile = zip_proposta, mode = "wb")
download.file(url_convenio, destfile = zip_convenio, mode = "wb")

# Descompactar dentro da pasta central
unzip(zip_proposta, exdir = "dados_siconv")
unzip(zip_convenio, exdir = "dados_siconv")

# Identificar os arquivos CSV
csvs <- list.files("dados_siconv", pattern = "\\.csv$", full.names = TRUE)
csv_proposta <- csvs[grepl("proposta", csvs)]
csv_convenio <- csvs[grepl("convenio", csvs)]

# Ler os dados com codificação Latin1 (ISO-8859-1)

dados_proposta <- read_delim(csv_proposta, delim = ";", locale = locale(encoding = "UTF-8"), show_col_types = FALSE)
dados_convenio <- read_delim(csv_convenio, delim = ";", locale = locale(encoding = "UTF-8"), show_col_types = FALSE)

# Verificar nomes das colunas
print(names(dados_proposta))
print(names(dados_convenio))

# Ajustar nomes se necessário
# names(dados_proposta)[names(dados_proposta) == "ID_PROPOSTA"] <- "id_projeto"
# names(dados_convenio)[names(dados_convenio) == "ID_PROPOSTA"] <- "id_projeto"

# Mesclar os dados
dados_mesclados <- inner_join(dados_proposta, dados_convenio, by = "ID_PROPOSTA")

# Manter apenas os dados mesclados
rm(list = setdiff(ls(), "dados_mesclados"))

# Obter a data atual
data_atual <- Sys.Date()

# Extrair dia, mês e ano
dia <- format(data_atual, "%d")
mes <- format(data_atual, "%m")
ano <- format(data_atual, "%Y")

# Criar a variável fonte_carga
fonte_carga <- paste0("SICONV/MPOG ", dia, "-", mes, "-", ano)

dados_mesclados$fonte_carga <- fonte_carga

# Criar a nova coluna 'link' com base no número do convênio
dados_mesclados$link <- paste0(
  "https://www.convenios.gov.br/siconv/ConsultarProposta/ResultadoDaConsultaDeConvenioSelecionarConvenio.do?sequencialConvenio=",
  dados_mesclados$NR_CONVENIO,
  "&Usr=guest&Pwd=guest"
)

# Criar a nova coluna com base na lógica fornecida
dados_mesclados$cd_status_projeto <- dplyr::case_when(
  dados_mesclados$SIT_CONVENIO %in% c(
    "Proposta/Plano de Trabalho Aprovado",
    "Proposta/Plano de Trabalho Complementado Enviado para Análise"
  ) ~ 3,
  
  dados_mesclados$SIT_CONVENIO %in% c(
    "Em execução",
    "Assinado",
    "Assinado Pendente Registro TV Siafi"
  ) ~ 4,
  
  dados_mesclados$SIT_CONVENIO %in% c(
    "Inadimplente",
    "Aguardando Prestação de Contas",
    "Prestação de Contas Aprovada",
    "Prestação de Contas Aprovada com Ressalvas",
    "Prestação de Contas em Análise",
    "Prestação de Contas em Complementação",
    "Prestação de Contas enviada para Análise",
    "Prestação de Contas Iniciada Por Antecipação",
    "Prestação de Contas Rejeitada"
  ) ~ 2,
  
  dados_mesclados$SIT_CONVENIO %in% c(
    "Cancelado",
    "Convênio Anulado"
  ) ~ 1,
  
  TRUE ~ 4  # valor padrão
)

# Substituir - por vazio na coluna CEP_PROPONENTE
dados_mesclados$CEP_PROPONENTE <- gsub("-", "", dados_mesclados$CEP_PROPONENTE)

# Substituir , por . nas colunas de valores monetários
colunas_valores <- c(
  "VL_CONTRAPARTIDA_CONV", "VL_CONTRAPARTIDA_PROP", "VL_DESEMBOLSADO_CONV",
  "VL_EMPENHADO_CONV", "VL_GLOBAL_CONV", "VL_GLOBAL_PROP",
  "VL_INGRESSO_CONTRAPARTIDA", "VL_REPASSE_CONV", "VL_REPASSE_PROP",
  "VL_SALDO_REMAN_CONVENENTE", "VL_SALDO_REMAN_TESOURO"
)

# Substituir vírgula por ponto
dados_mesclados[colunas_valores] <- lapply(dados_mesclados[colunas_valores], function(x) {
  as.numeric(gsub(",", ".", x))
})

# Substituir aspas simples ' por aspas duplas " na coluna OBJETO_PROPOSTA
dados_mesclados$OBJETO_PROPOSTA <- gsub("'", "\"", dados_mesclados$OBJETO_PROPOSTA)

# Salvar resultado
write.csv(dados_mesclados, "dados_siconv/dados_mesclados.csv", row.names = FALSE, fileEncoding = "UTF-8")


# Excluir tudo (descomente a linha abaixo se quiser apagar tudo ao final)
# unlink("dados_siconv", recursive = TRUE)
