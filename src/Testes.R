


QueryTeste <- "UPDATE tb_osc_teste 

SET  ft_apelido_osc = CASE id_osc
 WHEN '395953' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395954' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395955' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395958' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395960' THEN 'CNPJ/SRF/MF/2023_01'
END,

ft_identificador_osc = CASE id_osc
 WHEN '395953' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395954' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395955' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395957' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395958' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395959' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395960' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395961' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395962' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395963' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395964' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395965' THEN 'CNPJ/SRF/MF/2023_01'
END,

bo_osc_ativa = CASE id_osc
 WHEN '396041' THEN (FALSE)
 END, 

ft_osc_ativa = CASE id_osc
 WHEN '395953' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395954' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395955' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395957' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '395958' THEN 'CNPJ/SRF/MF/2023_01'
END

WHERE id_osc IN ('396041', '395953', '395954' ,'395955' ,'395957' ,'395958' ,
'395959' ,'395960' ,'395961' ,'395962' ,'395963' ,
'395964' ,'395965')"

cat(QueryTeste)

dbSendQuery(Conexao, QueryTeste)


QueryTeste <- "UPDATE tb_osc_teste 

SET  ft_osc_ativa = CASE id_osc
 WHEN '395953' THEN 'CNPJ/SRF/MF/2023_01'
 WHEN '396041' THEN 'CNPJ/SRF/MF/2023_01'
 END, 
 
bo_osc_ativa = CASE id_osc
 WHEN '396041' THEN TRUE
 END

WHERE id_osc IN ('396041', '395953')"

cat(QueryTeste)

x <- dbSendQuery(Conexao, QueryTeste)
x

dbClearResult(x)

QueryTeste <- "
SELECT count(*)
FROM INFORMATION_SCHEMA.COLUMNS;"
cat(QueryTeste)

x <- dbSendQuery(connec, QueryTeste)
x

dbClearResult(x)








sum(is.na(DadosAntigos$tx_apelido_osc))

length(unique(AddData$tx_apelido_osc[!is.na(AddData$tx_apelido_osc)]))
length(AddData$tx_apelido_osc[!is.na(AddData$tx_apelido_osc)])


dbExecute(Conexao, "\d tb_osc")

dbta

AddData[["tx_apelido_osc"]] <- NA


names(DadosNovos)


class(DadosNovos$dt_ano_cadastro_cnpj)
class(DadosAntigos$dt_ano_cadastro_cnpj)


head(DadosNovos$dt_ano_cadastro_cnpj[!is.na(DadosNovos$dt_ano_cadastro_cnpj)])
head(DadosAntigos$dt_ano_cadastro_cnpj[!is.na(DadosAntigos$dt_ano_cadastro_cnpj)])




head(DadosNovos$dt_fundacao_osc[!is.na(DadosNovos$dt_fundacao_osc)])
head(DadosAntigos$dt_fundacao_osc[!is.na(DadosAntigos$dt_fundacao_osc)])

DadosAntigos <- dbGetQuery(Conexao, 
                           paste0("SELECT * FROM ",
                                  Table_NameAntigo,
                                  # " LIMIT 500", 
                                  ";"))

class(Alteracao[["Dado"]])

str_detect(Alteracao[["Dado"]], "'")

"UPDATE tb_dados_gerais 
dt_fundacao_osc = CASE id_osc
WHEN '786744' THEN '11820'
WHEN '789420' THEN '11822'
END 
WHERE id_osc IN ('786744', '789420')"
