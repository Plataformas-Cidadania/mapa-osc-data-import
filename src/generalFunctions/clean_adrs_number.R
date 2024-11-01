# Função para limpar e uniformizar os números de endereços

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-09-24

# Debug
# x <- c("1.518", "2-8", "12 C 3", "S/N", "KM 12")

# Função  - Início ####

clean_adrs_number <- function(x) {
  
  output <- as.character(x)
  
  retirar1 <- c(
    # Retirar complementos
    "PREDIO.*", "CASA.*", "BLOCO.*", "BL.*", "LOTE.*",
    "BOX.*", "BX.*", "QUADRA.*", 
    # Eliminar casos assim: "4-73", "150/52"
    "-.*", "/.*"  )
  
  retirar_tudo <- c("[:punct:]", "[:alpha:]")
  retirar2 <- c(" .*")
  
  for (i in seq_along(retirar1)) {
    output <- str_remove(output, retirar1[[i]])
  }
  
  for (i in seq_along(retirar_tudo)) {
    output <- str_remove_all(output, retirar_tudo[[i]])
  }
  
  for (i in seq_along(retirar2)) {
    output <- str_remove(output, retirar2[[i]])
  }
  rm(i)
  
  # Remove casos assim: "complemento 10" ('complemento' pode ser qualquer coisa)
  output <- str_remove(output, " .*")
  
  # Remove espaços
  output <- str_remove_all(output, " ")
  output <- str_trim(output)
  
  # Detecta casos sem número:
  output <- ifelse(output == "0" | output == "", "SN", output)
  
  # Detecta casos de número que quilômetros
  output[str_detect(x, "KM")] <- "KM"
  
  return(output)
  
  rm(output, x, retirar1, retirar2, retirar_tudo)
}

# Fim ####