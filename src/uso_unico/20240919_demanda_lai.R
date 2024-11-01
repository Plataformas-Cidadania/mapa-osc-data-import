# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: atender a demanda da LAI colocada no dia 19/09/2024 15:21
# para fornecer uma base de dados com informações detalhadas das OSC 
# (ver variáveis abaixo)

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-09-19

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Demanda ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# @Murilo de Oliveira Junqueira a urgência dessa ação é porque também precisamos 
# responder a um pedido de informações via LAI. Mas é algo que há meses já temos 
# planejado.

# A ação consiste em extrair um arquivo, a ser disponibilizado para download na 
# seção ‘base de dados’ do Mapa das OSCs. Mapa das OSC - Base de Dados 
# (ipea.gov.br). O arquivo deve ser zipado, em formato .csv. Estou considerando
# que o arquivo com o dicionário de variáveis já está pronto e será 
# disponibilizado em outro arquivo, no mesmo ambiente de bases de dados.

# Deve incluir as seguintes variáveis :

# ano de referência da base
# código do município
# nome do município
# código da UF
# nome da UF
# natureza jurídica
# CNPJ
# data de fundação da OSC
# ano de início das atividades da OSCs (se houver) (não tem)
# razão social
# nome fantasia
# endereço completo
# latitude e longitude, se houver
# finalidades de atuação/area de atuação (CNAE primária e secundária).
# (todas as variáveis relativas à finalidade de atuação (incluídos os códigos 
# e classes CNAE, devem ser incluídas).

# NOTA: Para ilustrar, anexo planilha compartilhada via LAI no ano passado Note que há campos que não vamos disponibilizar dessa vez.
# @Alexandre Domingues


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Geração da base ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Dados da tabela 'tb_dados_gerais':

tb_dados_gerais <- readRDS("backup_files/2023_01/output_files/tb_dados_gerais.RDS")

Dados <- tb_dados_gerais %>% 
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
         dt_fundacao_osc)

rm(tb_dados_gerais)


# Dados da tabela 'tb_localizacao':

tb_localizacao <- readRDS("backup_files/2023_01/output_files/tb_localizacao.RDS")

Dados <- Dados %>% 
  left_join(select(tb_localizacao, 
                   id_osc,
                   # código do município
                   cd_municipio,
                   # nome do município
                   tx_endereco_corrigido2,
                   # endereço completo
                   tx_endereco_corrigido,
                   # latitude e longitude, se houver
                   tx_latitude,
                   tx_longitude), 
            by = "id_osc") %>% 
  # Corrige nomes esquisitos desta base:
  rename(municipio_nome = tx_endereco_corrigido2, 
         tx_endereco_completo = tx_endereco_corrigido) %>% 
  mutate(
    # código da UF
    cd_uf = str_sub(cd_municipio, 1, 2),
    # nome da UF
    sigla_uf = str_sub(municipio_nome, -2, -1)  )

rm(tb_localizacao)


# Dados da tabela 'tb_area_atuacao'
# finalidades de atuação/area de atuação 


tb_area_atuacao <- readRDS("backup_files/2023_01/output_files/tb_area_atuacao.RDS")

unique(tb_area_atuacao$cd_area_atuacao)

names(tb_area_atuacao)


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

DB_OSC <- readRDS("backup_files/2023_01/intermediate_files/DB_OSC.RDS")

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
  # Deixa as variáveis em uma ordem agradável
  select(
    # Identificação da OSC:
    cnpj, 
    tx_razao_social_osc, 
    tx_nome_fantasia_osc, 
    cd_natureza_juridica_osc, 
    dt_fundacao_osc, 
    
    # Localização:
    tx_endereco_completo, 
    cd_municipio, 
    municipio_nome, 
    cd_uf, 
    sigla_uf, 
    tx_latitude, 
    tx_longitude, 
    
    # Áreas de Atuação:
    cnae, 
    cnae_fiscal_secundaria,
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

# Salva
fwrite(Dados2, "data/temp/20240919_demanda_lai.csv", 
       sep = ";", dec = ",")

rm(Dados, Dados2)


# Fim ####