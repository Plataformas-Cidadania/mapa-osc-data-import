# Scrip Análise preliminar de dados RAIS

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-06-20


DataDir <- "data/raw/RAIS/"

RawDataFiles <- list.files(DataDir, 
                           "^RAIS_Estabelecimentos_Data_")

# Lista de UFs
UFs <- fread("tab_auxiliares/UFs.csv", encoding = "Latin-1")

dataTable <- tibble()

for (i in seq_along(RawDataFiles)) {
  # i <- 1
  message(RawDataFiles[i])
  
  RawData <- readRDS(paste0(DataDir, RawDataFiles[i]))
  
  newdata <- RawData %>% 
    group_by(ano) %>% 
    summarise(TotalCLT = sum(qtde_vinc_ativos, na.rm = TRUE))
  
  dataTable <- bind_rows(dataTable, newdata)
  rm(RawData, newdata)
  
}
rm(i)


dataTable

ggplot(data = dataTable, 
       aes(x = as.integer(ano), y = TotalCLT)) + 
  geom_line(color = "red", linewidth = 1.2) + 
  geom_point(color = "blue", size = 3 ) + 
  scale_x_continuous(breaks = 2010:2020) + 
  scale_y_continuous(labels = scales::comma_format(big.mark = ".")) + 
  theme_classic() + 
  xlab("Ano") + 
  ylab("Número de Empregos CLT Ativos")
