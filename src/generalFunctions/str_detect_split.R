# Função para detectar expressões regulares em chunks

# Autor: Murilo Junqueira (m.junqueira@yahoo.com.br)

# Data da Criação: 2023-10-26

library(stringr)

# Debug:
# chunksize = 20
# pattern <- check_names
# x <- TestData[[VarTest]]

str_detect_split <- function(x, pattern, chunksize = 20) {
  
  # Divide os padrões em chunk
  patternchunks <- split(pattern, ceiling(seq_along(pattern)/chunksize))
  
  # Cria vetor para guardar os resultados
  testpattern <- rep(FALSE, length(x))
  
  # Teste cada chunk
  for (i in seq_along(patternchunks)) {
    # i <- 1
    patterchunk_i <- paste0(patternchunks[[i]], collapse = "|")
    testpattern <- testpattern | str_detect(x, patterchunk_i)
  }
  
  # sum(testpattern)
  # x[testpattern]
  
  return(testpattern)
  rm(i, patterchunk_i, testpattern, patternchunks)
  rm(chunksize, pattern, x)
}

# End