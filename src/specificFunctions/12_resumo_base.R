# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: criar uma versão simplificada da base de dados que pode ser
# exportada por um arquivo CSV grande.

# Baseado no script: 20240919_demanda_lai.R

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-12-04

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup #### 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)
library(DBI)
library(RODBC)
library(RPostgres)

message("Iniciando extração de dados para CSV")
Sys.sleep(2)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria conexão com a base ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Conecta ao banco de dados, se necessário:
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))) {
  
  # Função para facilitar a conexão com os bancos de dados PostgreSQL:
  source("src/generalFunctions/postCon.R") 
  
  # Concecta aos bancos de dados do MOSC:
  conexao_mosc <- postCon(definicoes$credenciais_mosc, Con_options = "-c search_path=osc")
  
  rm(postCon)
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Geração da base ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Usando o arquivo de backup:
# tb_osc <- readRDS("backup_files/2024_01/output_files/tb_osc.RDS")

# Usando o arquivo do banco de dados
tb_osc <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_osc",
                                                   # " LIMIT 500", 
                                                   ";"))

# Dados da tabela 'tb_dados_gerais':

# Usando o arquivo de backup:
# tb_dados_gerais <- readRDS("backup_files/2024_01/output_files/tb_dados_gerais.RDS")

# Usando o arquivo do banco de dados
tb_dados_gerais <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_dados_gerais",
                                  # " LIMIT 500", 
                                  ";"))

# names(tb_dados_gerais)

Dados <- tb_dados_gerais %>% 
  left_join(select(tb_osc, id_osc, cd_identificador_osc, bo_osc_ativa, 
                   cd_situacao_cadastral), 
            by = "id_osc") %>% 
  # dplyr::filter(bo_osc_ativa) %>% # desde a att 2026_01, inserir as osc inativas
  # Evita problemas de padding:
  mutate(
    cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                   width = 14, 
                                   pad = "0"), 
    situacao_cadastral = case_when(
      bo_osc_ativa ~ "Nula ou Baixada",
      cd_situacao_cadastral == 2 ~ "Ativa",
      cd_situacao_cadastral == 3 ~ "Suspensa",
      cd_situacao_cadastral == 4 ~ "Inapta",
      TRUE ~ "Não Identificado"
      ),
    removida_do_mosc = ifelse(bo_osc_ativa, "não", "sim"),
    matriz_filial = case_when(
      cd_matriz_filial == 1 ~ "Matriz",
      cd_matriz_filial == 2 ~ "Filial",
      TRUE ~ "Não Identificado"
    ),
  ) %>% 
  select(id_osc, 
         # razão social
         tx_razao_social_osc, 
         # CNPJ
         cd_identificador_osc, 
         # natureza jurídica
         cd_natureza_juridica_osc,  
         # nome fantasia
         tx_nome_fantasia_osc, 
         # data de fundação da OSC
         dt_fundacao_osc, 
         # cnae
         cd_classe_atividade_economica_osc, 
         # Situação cadastral
         situacao_cadastral, 
         # Se a osc está presente no mapa ou não
         removida_do_mosc,
         # Mostra se é matriz ou filial
         matriz_filial,
         # Data de Fechamento
         dt_fechamento_osc,
         # Ano de Fechamento
         nr_ano_fechamento_osc,
         # CNAE secundária
         cd_cnae_secundaria,
         )

rm(tb_dados_gerais, tb_osc)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere dados de localização ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Dados da tabela 'tb_localizacao':

# Usando o arquivo de backup:
# tb_localizacao <- readRDS("backup_files/2024_01/output_files/tb_localizacao.RDS")

message("Extraindo geolocalização")

# Usando o arquivo do banco de dados
tb_localizacao <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_localizacao",
                                                   # " LIMIT 500", 
                                                   ";"))

# sum(is.na(tb_localizacao$tx_endereco_corrigido))
# sum(is.na(Dados$tx_endereco_completo))
# names(tb_localizacao)

Municipios <- fread("tab_auxiliares/Municipios.csv", 
                    encoding = "Latin-1")

UFs <- fread("tab_auxiliares/UFs.csv", 
             encoding = "Latin-1")


LatLonOSC <- dbGetQuery(conexao_mosc, 
                        glue(
                          "
  SELECT
    id_osc,
    public.ST_X(geo_localizacao) AS longitude,
    public.ST_Y(geo_localizacao) AS latitude
  FROM
    tb_localizacao;
  "
                        )
                        )

message("Processando dados...")

# View(LatLonOSC)

Dados <- Dados %>% 
  left_join(select(tb_localizacao, 
                   id_osc,
                   # código do município
                   cd_municipio,
                   # endereço completo
                   tx_endereco_corrigido), 
            by = "id_osc") %>% 
  
  left_join(select(Municipios, Munic_Id, UF_Id, Munic_Nome), 
            by = c("cd_municipio" = "Munic_Id")) %>% 
  
  left_join(UFs, by = "UF_Id") %>% 
  
  left_join(LatLonOSC, by = "id_osc") %>% 
            
  # Corrige nomes esquisitos desta base:
  rename(tx_endereco_completo = tx_endereco_corrigido) %>% 
  
  select(everything())

# names(Dados)

rm(tb_localizacao, Municipios, UFs, LatLonOSC)




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere dados de contato ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Dados de contato")

tb_contato <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_contato",
                                              # " LIMIT 500", 
                                              ";"))
# names(tb_contato)

Dados <- Dados %>% 
  # Insere as dummies de área de atuação no conjunto principal de dados:
  left_join(select(tb_contato, 
                   id_osc, 
                   tx_telefone,
                   tx_email, 
                   nm_representante,
                   tx_site,    
                   tx_facebook,        
                   tx_google,          
                   tx_linkedin,        
                   tx_twitter,
  ), 
  by = "id_osc")

rm(tb_contato)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Insere dados de área de atuação ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Dados de área de atuação")

# Dados da tabela 'tb_area_atuacao'
# finalidades de atuação/area de atuação 

# Usando o arquivo de backup:
# tb_area_atuacao <- readRDS("backup_files/2024_01/output_files/tb_area_atuacao.RDS")

# Usando o arquivo do banco de dados
tb_area_atuacao <- dbGetQuery(conexao_mosc, paste0("SELECT * FROM tb_area_atuacao",
                                                  # " LIMIT 500", 
                                                  ";"))

## Tabelas auxiliares (descrição dos códigos das áreas de atuação):
dc_area_atuacao <- fread("tab_auxiliares/dc_area_atuacao.csv", encoding = "Latin-1")
dc_subarea_atuacao <- fread("tab_auxiliares/dc_subarea_atuacao.csv", encoding = "Latin-1") %>% 
  # Tirar ambiguidade sobre duas classificações "Outras"
  distinct(cd_subarea_atuacao, .keep_all = TRUE)


# Banco de dados para processar base das áreas de atuação antes de inserir
# nos dados gerais:
dados_join <- tb_area_atuacao %>% 
  # slice(1:20000) %>% # para teste apenas
  # Adiciona os textos das áreas e subareas de atuação
  left_join(dc_area_atuacao, by = "cd_area_atuacao") %>% 
  left_join(select(dc_subarea_atuacao, 
                   cd_subarea_atuacao, 
                   tx_subarea_atuacao), 
            by = "cd_subarea_atuacao") %>% 
  # Vou criar uma dummy para cada área e subárea de atuação:
  mutate(tx_subarea_atuacao = paste0("SubArea_", tx_subarea_atuacao), 
         tx_area_atuacao = paste0("Area_", tx_area_atuacao)) %>% 
  select(id_osc, tx_area_atuacao, tx_subarea_atuacao) %>% 
  # Une área e subárea em uma coluna só:
  gather(key, value, tx_area_atuacao:tx_subarea_atuacao) %>% 
  # Remove os missing:
  dplyr::filter(value != "SubArea_NA", 
                value != "Area_NA") %>% 
  select(-key) %>% 
  # Cria as dummies:
  mutate(flag = 1) %>% 
  # Deixa apenas uma vez o valor da área e Subárea:
  distinct(id_osc, value, .keep_all = TRUE) %>% 
  # Corrige nomes para eles ficarem adequadas a rótulos de Variáveis
  mutate(
    value = str_replace_all(value, "/", "_"),
    value = str_replace_all(value, " ", "_"),
    value = str_remove_all(value, fixed(",")),
    # remove acentos:
    value = stringi::stri_trans_general(value, id = "Latin-ASCII")
  ) %>% 
  spread(key = value, value = flag, fill = 0)

# names(dados_join)

Dados <- Dados %>% 
  # Insere as dummies de área de atuação no conjunto principal de dados:
  left_join(dados_join, by = "id_osc")

rm(dados_join, tb_area_atuacao)
rm(dc_area_atuacao, dc_subarea_atuacao)


# Dados do arquivo da RFB: ####
# (CNAE primária e secundária).

# DB_OSC
# diretorio_att <- "backup_files/2025_03/"

# TODO: Verificar como adicionar a variável da CNAE secundária ####
if(FALSE) {
  assert_that(file.exists(glue("{diretorio_att}intermediate_files/DB_OSC.RDS")))
  
  DB_OSC <- readRDS(glue("{diretorio_att}intermediate_files/DB_OSC.RDS"))
  
  Dados <- Dados %>% 
    # Vou deixar o CNPJ mais claro aqui:
    rename(cnpj = cd_identificador_osc) %>% 
    left_join(select(DB_OSC, 
                     cnpj,
                     cnae,
                     cnae_fiscal_secundaria),
              by = "cnpj" ) %>% 
    mutate(cnae_fiscal_secundaria = str_replace_all(cnae_fiscal_secundaria, 
                                                    ",", fixed(" | ")))
  
  rm(DB_OSC)
}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Arruma e Salva os dados finais ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Arruma e Salva os dados finais")

# names(Dados)

Dados2 <- Dados %>% 
  rename(
    cnpj = cd_identificador_osc, 
    municipio_nome = Munic_Nome, 
    cnae = cd_classe_atividade_economica_osc,
    data_fechamento = dt_fechamento_osc,
    ano_fechamento = nr_ano_fechamento_osc,
    cnae_secundaria = cd_cnae_secundaria,
    natureza_juridica = cd_natureza_juridica_osc,
    telefone = tx_telefone,
    email = tx_email, 
    nome_representante = nm_representante,
    site = tx_site,    
    facebook = tx_facebook,        
    site_google = tx_google,          
    linkedin = tx_linkedin,        
    twitter = tx_twitter,
    ) %>% 
  
  # Deixa as variáveis em uma ordem agradável
  select(
    
    # Identificação da OSC:
    cnpj, 
    tx_razao_social_osc, 
    tx_nome_fantasia_osc, 
    
    # Dados da OSC:
    natureza_juridica, 
    matriz_filial,
    situacao_cadastral,
    dt_fundacao_osc, 
    removida_do_mosc,
    data_fechamento,
    ano_fechamento,
    
    # Localização:
    tx_endereco_completo, 
    cd_municipio, 
    municipio_nome, 
    # cd_uf, 
    UF_Sigla, 
    longitude, 
    latitude, 
    
    # Contato:
    telefone,
    email, 
    nome_representante,
    site,    
    facebook,        
    site_google,          
    linkedin,        
    twitter,
    
    # Áreas de Atuação:
    cnae, 
    cnae_secundaria,
    Area_Assistencia_social, 
    Area_Associacoes_patronais_e_profissionais, 
    Area_Cultura_e_recreacao, 
    Area_Desenvolvimento_e_defesa_de_direitos_e_interesses, 
    Area_Educacao_e_pesquisa, 
    Area_Outras_atividades_associativas, 
    Area_Religiao, 
    Area_Saude, 
    SubArea_Assistencia_social, 
    SubArea_Associacoes_de_atividades_nao_especificadas_anteriormente, 
    SubArea_Associacoes_de_produtores_rurais_pescadores_e_similares, 
    SubArea_Associacoes_empresariais_e_patronais, 
    SubArea_Associacoes_profissionais, 
    SubArea_Atividades_de_apoio_a_educacao, 
    SubArea_Cultura_e_arte, 
    SubArea_Desenvolvimento_e_defesa_de_direitos, 
    SubArea_Educacao_infantil, 
    SubArea_Educacao_profissional, 
    SubArea_Ensino_fundamental, 
    SubArea_Ensino_superior, 
    SubArea_Esportes_e_recreacao, 
    SubArea_Estudos_e_pesquisas, 
    SubArea_Hospitais, 
    SubArea_Outras_formas_de_educacao_ensino, 
    SubArea_Outros_servicos_de_saude, 
    SubArea_Religiao)

# names(Dados2)
# 
# table(Dados2$SubArea_Hospitais)
# table(Dados2$SubArea_Esportes_e_recreacao)
# table(Dados2$SubArea_Educacao_infantil)
# table(Dados2$Area_Cultura_e_recreacao)
# table(Dados2$Area_Desenvolvimento_e_defesa_de_direitos_e_interesses)
# table(Dados2$Area_Religiao)


hoje_txt <- today() %>% 
  as.character() %>% 
  str_remove_all("-")

message("Salvando o arquivo")

# Salva base completa

fwrite(Dados2, glue("{diretorio_att}output_files/{hoje_txt}_MOSC_basecompleta.csv"), 
       sep = ";", dec = ",")

saveRDS(Dados2, 
        glue("{diretorio_att}output_files/{hoje_txt}_MOSC_basecompleta.RDS"))

# Salva base sem os contatos
Dados_semContato <- Dados2 %>% 
  select(
    -telefone,
    -email, 
    -nome_representante,
    -site,    
    -facebook,        
    -site_google,          
    -linkedin,        
    -twitter,)

fwrite(Dados_semContato, 
       glue("{diretorio_att}output_files/{hoje_txt}_MOSC_baseresumida.csv"), 
       sep = ";", dec = ",")

rm(Dados, Dados2, Dados_semContato)
dbDisconnect(conexao_mosc)
rm(conexao_mosc)
rm(hoje_txt)
gc()


# Fim ####