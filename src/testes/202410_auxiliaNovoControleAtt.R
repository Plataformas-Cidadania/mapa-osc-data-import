# Atualiza controle de processos (tb_processos_atualizacao)  
if(!definicoes$att_teste) atualiza_processos_att(
  TipoAtt = "inicio", 
  id_att = id_presente_att, 
  id_processo = 2, 
  processo_nome = "Identificação OSC")


# Atualiza controle de processos (tb_processos_atualizacao)  
if(!definicoes$att_teste) atualiza_processos_att(
  TipoAtt = "fim", 
  id_att = id_presente_att, 
  id_processo = 5, 
  path_file_backup = ifelse(definicoes$salva_backup, path_file_backup, NULL))


# Salva Backup:
if(definicoes$salva_backup) {
  
  path_file <- glue("{diretorio_att}intermediate_files/tb_osc.RDS")
  saveRDS(tb_osc, path_file)
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "arquivo backup", 
    id_att = id_presente_att, 
    id_processo = 6, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              path_file, 
                              NULL))
  
  rm(path_file)
}



tb_backups_files

tb_processos_atualizacao


names(tb_localizacao)


sum(!is.na(tb_localizacao$geom))

nrow(LatLon_data) - 30574


atualiza_processos_att(
  TipoAtt = "remove linha backups_files", 
  id_att = id_presente_att, 
  id_processo = 6, 
  path_file_backup = ifelse(definicoes$salva_backup, 
                            path_file, 
                            NULL))



tb_relacoes_trabalho <- tbl(conexao_mosc, "tb_relacoes_trabalho") %>% 
  collect() 



sum(is.na(tb_osc$id_osc))


tb_osc_OLD <- tbl(conexao_mosc, "tb_osc") %>% 
  collect() 


tb_localizacao_OLD <- tbl(conexao_mosc, "tb_localizacao") %>% 
  collect() 


names(tb_osc_OLD)
names(tb_localizacao_OLD)


tb_osc_OLD$bo_osc_ativa[1:10]


teste <- tb_osc_OLD %>% 
  left_join(select(tb_localizacao_OLD, id_osc, cd_municipio), 
            by = "id_osc") %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  mutate(nchar = nchar(as.character(cd_municipio)))


sum(is.na(teste$cd_municipio))


table(teste$nchar)

rm(tb_localizacao_OLD2)

source("src/generalFunctions/postCon.R") 

# Concecta aos bancos de dados do MOSC:
conexao_mosc2 <- postCon("keys/psql12-prod_key.json", 
                        Con_options = "-c search_path=osc")

# Testa extrair dados do banco
tb_osc_OLD <- try(dbGetQuery(conexao_mosc2, 
                            glue("SELECT * FROM tb_osc",
                                 #" LIMIT 500", 
                                 ";")))

tb_localizacao_OLD <- try(dbGetQuery(conexao_mosc2, 
                                     glue("SELECT * FROM tb_localizacao",
                                          #" LIMIT 500", 
                                          ";")))

teste <- tb_osc_OLD %>% 
  left_join(select(tb_localizacao_OLD, id_osc, cd_municipio), 
            by = "id_osc") %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  mutate(nchar = nchar(as.character(cd_municipio)))


sum(is.na(teste$cd_municipio))

table(teste$nchar, useNA = "always")


sum(is.na(teste$cd_municipio == "0"))

rm(teste, teste_rfb, tb_osc_OLD, tb_localizacao_OLD, conexao_mosc2, postCon)
rm(KeyFile, keys)
rm(path_file, path_file_backup)
