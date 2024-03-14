

# Função para criar tabelas de teste de bancos de dados

# Debug:
# tabName <- "tb_osc"
# suffix = "_teste"

# CriaTbTeste(connec, "tb_osc")
# CriaTbTeste(connec, "tb_dados_gerais")
# CriaTbTeste(connec, "tb_contato")
# # # CriaTbTeste(connec, "tb_osc")
# CriaTbTeste(connec, "tb_area_atuacao")

CriaTbTeste <- function(connec, tabName, suffix = "_teste") {
  
  # Verifica a coneção com a base
  assert_that(dbIsValid(connec))
  
  # Verifica se a tabela está presente no BD
  Tables <- dbListTables(connec)
  assert_that(tabName %in% Tables)
  
  # Baixa dados da tabela original
  tab_testeData <- dbGetQuery(connec, paste0("SELECT * FROM ", tabName, ";"))
  
  # Nome da tabela teste
  tab_testeName <- paste0(tabName, suffix)
  
  # Remove a tabela prévia, se tiver
  if(dbExistsTable(connec, tab_testeName)) dbRemoveTable(connec, tab_testeName)
  
  # Acrescenta a tabela nova
  dbWriteTable(connec, tab_testeName, tab_testeData)
  
  # Verifica se funcionou
  assert_that(dbExistsTable(connec, tab_testeName))
  
  return(TRUE)
}
