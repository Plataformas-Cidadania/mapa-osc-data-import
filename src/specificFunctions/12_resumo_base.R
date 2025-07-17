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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria conexão com a base ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Conecta ao banco de dados, se necessário:
if(!(exists("conexao_mosc") && dbIsValid(conexao_mosc))) {
  
  # Credenciais de acesso ao banco de dados:
  # (determinam qual banco de dados acessar)
  # credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"
  # credenciais_mosc <- "keys/psql12-homolog_keySUPER.json"
  credenciais_mosc <- "keys/psql12-usr_manutencao_mapa.json" # acesso completo ao banco de produção
  
  # Função para facilitar a conexão com os bancos de dados PostgreSQL:
  source("src/generalFunctions/postCon.R") 
  
  # Concecta aos bancos de dados do MOSC:
  conexao_mosc <- postCon(credenciais_mosc, Con_options = "-c search_path=osc")
  
  rm(credenciais_mosc, postCon)
  
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

names(tb_dados_gerais)

Dados <- tb_dados_gerais %>% 
  left_join(select(tb_osc, id_osc, cd_identificador_osc, bo_osc_ativa, 
                   cd_situacao_cadastral), 
            by = "id_osc") %>% 
  dplyr::filter(bo_osc_ativa) %>% 
  # Evita problemas de padding:
  mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                        width = 14, 
                                        pad = "0")) %>% 
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
         cd_situacao_cadastral
         )

rm(tb_dados_gerais, tb_osc)


# Dados da tabela 'tb_localizacao':

# Usando o arquivo de backup:
# tb_localizacao <- readRDS("backup_files/2024_01/output_files/tb_localizacao.RDS")


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
            
  # Corrige nomes esquisitos desta base:
  rename(tx_endereco_completo = tx_endereco_corrigido) %>% 
  
  select(everything())

# names(Dados)

rm(tb_localizacao, Municipios, UFs)


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

DB_OSC <- readRDS("backup_files/2025_03/intermediate_files/DB_OSC.RDS")

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


# Arruma e Salva os dados finais:

names(Dados)

Dados2 <- Dados %>% 
  rename(cnpj = cd_identificador_osc, 
         municipio_nome = Munic_Nome, 
         cnae = cd_classe_atividade_economica_osc) %>% 
  # Deixa as variáveis em uma ordem agradável
  select(
    # Identificação da OSC:
    cnpj, 
    tx_razao_social_osc, 
    tx_nome_fantasia_osc, 
    
    
    # Dados da OSC:
    cd_natureza_juridica_osc, 
    dt_fundacao_osc, 
    cd_situacao_cadastral,
    
    # Localização:
    tx_endereco_completo, 
    cd_municipio, 
    municipio_nome, 
    # cd_uf, 
    UF_Sigla, 
    # tx_latitude, 
    # tx_longitude, 
    
    # Áreas de Atuação:
    cnae, 
    # cnae_fiscal_secundaria,
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

# Salva
fwrite(Dados2, glue("backup_files/2025_03/output_files/{hoje_txt}_MOSC_baseresumida.csv"), 
       sep = ";", dec = ",")

rm(Dados, Dados2)
dbDisconnect(conexao_mosc)
rm(conexao_mosc)
rm(hoje_txt)
gc()


# Fim ####