
uf <- "23_CE"
uf <- "33_RJ"
uf <- "29_BA"

# Rotina de leitura de arquivos grandes

uf <- "29_BA"
file_csv <- glue("{input_folder}{uf}.csv")

# Seleciona apenas colunas relevantes:
# "COD_MUNICIPIO", "CEP", "NUM_ENDERECO", "LATITUDE"
# "LONGITUDE"
selColunas <- list(integer = c(3, 9, 14),
                   numeric = c(26, 27))

# Coloca o nome das colunas
coluna_nomes <- c("COD_MUNICIPIO", "CEP", "NUM_ENDERECO", 
                  "LATITUDE", "LONGITUDE")

# Já coloca a classe delas
classcols <- c(rep("integer", 3), rep("numeric", 3))

# Calcula o número de linhas do arquivo
nlinhas <- readr::count_fields(
  file_csv, 
  tokenizer = readr::tokenizer_csv() ) %>% 
  length() %>% 
  magrittr::subtract(1)

# Máximo de linhas por chunk
max_rows <- 1000000

# Número de chunks
n_chunk <- nlinhas %/% max_rows + ifelse(nlinhas %% max_rows == 0, 0, 1)

# Controle dos chunks
chunks_df <- tibble(.rows = n_chunk) %>% 
  mutate(row_number = row_number(),
         n_rows = ifelse(row_number == max(row_number), 
                         nlinhas %% max_rows, max_rows),
         skip = row_number * max_rows - max_rows + 1)

# Teste de rows e skip:
data_chunk <- fread(glue("{input_folder}{uf}.csv"), 
                    nrows = chunks_df$n_rows[j], 
                    skip = chunks_df$skip[j], 
                    select = selColunas) %>% 
  magrittr::set_names(coluna_nomes)





# Teste de rows e skip:
teste1 <- fread(glue("{input_folder}{uf}.csv"), 
                nrows = 1000)

fwrite(teste1, "data/temp/testeSkip.csv")
teste2 <- fread("data/temp/testeSkip.csv", 
                skip = 1001)


# teste de checagem de número de linhas:
as.numeric(system("cat data/temp/testeSkip.csv | wc -l", intern = TRUE)) - 1 

file_csv <- "data/temp/testeSkip.csv"

file_csv <- glue("{input_folder}{uf}.csv")

filecode <- glue("wc -l {file_csv}")

as.integer(strsplit(system(filecode, 
                           intern = TRUE), " ")[[1]][[1]]) - 1

teste3 <- fread(glue("{input_folder}{uf}.csv"))

nrow(teste3)

rm(teste3)


# Calcula o número de linhas do arquivo
nlinhas <- readr::count_fields(
  file_csv, 
  tokenizer = readr::tokenizer_csv() ) %>% 
  length() %>% 
  magrittr::subtract(1)


# Teste de divisão por inteiro e resto:
14 %/% 3
14 %% 3

nlinhas %/% 1000000
nlinhas %% 1000000
