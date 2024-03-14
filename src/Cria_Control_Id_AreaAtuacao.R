
ft_area_atuacao_Temp <- fread("data/temp/ft_area_atuacao_Temp.csv") %>% 
  select(ft_area_atuacao, ft_area_atuacaoPadronizado)

names(ft_area_atuacao_Temp)

tb_area_atuacao_OLD <- dbGetQuery(connec, paste0("SELECT * FROM tb_area_atuacao",
                                                 # " LIMIT 1000",
                                                 ";"))
names(tb_area_atuacao_OLD)

Control_Id_AreaAtuacao <- tb_area_atuacao_OLD %>% 
  mutate(has_area = !is.na(cd_area_atuacao), 
         has_subarea = !is.na(cd_subarea_atuacao)) %>% 
  left_join(ft_area_atuacao_Temp, by = "ft_area_atuacao") %>% 
  distinct(id_osc, ft_area_atuacaoPadronizado, has_area, has_subarea, 
           .keep_all = TRUE) %>% 
  select(id_area_atuacao, id_osc, ft_area_atuacaoPadronizado, 
         has_area, has_subarea) %>% 
  select(everything())

saveRDS(Control_Id_AreaAtuacao, "tab_auxiliares/Control_Id_AreaAtuacao.RDS")  
