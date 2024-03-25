# Script para atualizar as informações do Mapa das Organizações da Sociedade Civil (MOSC)
# Script voltado para extração dos dados da Receita Federal (base dos CNPJs)

# Instituto de Economia Aplicada - IPEA

# Autor do Script: Murilo Junqueira (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2023-10-19

# Setup ####

message("Iniciando Atualização...")

message("Carregando bibliotecas...")
library(magrittr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringr)
library(assertthat)
library(readxl)
library(jsonlite)

# Variáveis importantes

message("Carregando controle de atualizações")

# Nome deste arquivo
ArquivoAtualizacao <- "src/OSC-v2023.R"
assert_that(file.exists(ArquivoAtualizacao), 
            msg = "Atualize caminho do arquivo de atualização!")

# Periodicidade das Atualizações (em dias)
ValidadeAtts <- 365

# Verifica a senha dos bancos de dados:
assert_that(file.exists("keys/rais_2019_key2.json"), 
            msg = "Não encontrado chave secreta do banco 'rais_2019'")

# Verifica se os arquivos necessários estão em tab_auxiliares

## Controle do ID colocado pelo último usuário
assert_that(file.exists("tab_auxiliares/idControl.RDS"), 
            msg = "O arquivo de ID das OSC da última versão não está disponível")

## Controle do ID colocado pelo último usuário
assert_that(file.exists("tab_auxiliares/idAreaAtuacaoControl.RDS"), 
            msg = "O arquivo de ID das área de atuação das OSC da última versão não está disponível")

## Nomes que indicam que determinado CNPJ não é OSC
assert_that(file.exists("tab_auxiliares/NonOSCNames.csv"), 
            msg = "O arquivo de ID das OSC da última versão não está disponível")

## Relação entre áreas e subáreas de atuação
assert_that(file.exists("tab_auxiliares/Areas&Subareas.csv"), 
            msg = "O arquivo de ID das OSC da última versão não está disponível")

## Critérios para determinar as áreas de atuação
assert_that(file.exists("tab_auxiliares/IndicadoresAreaAtuacaoOSC.csv"), 
            msg = "O arquivo de ID das OSC da última versão não está disponível")

## Veja se os descritórios dos códigos das áreas estão das tabelas auxiliares
assert_that(file.exists("tab_auxiliares/dc_area_atuacao.csv"), 
            msg = "O arquivo de 'dc_area_atuacao.csv' não está disponível")

## Veja se os descritórios dos códigos das áreas estão das tabelas auxiliares
assert_that(file.exists("tab_auxiliares/dc_subarea_atuacao.csv"), 
            msg = "O arquivo de 'dc_subarea_atuacao.csv' não está disponível")

## Veja se o "de/para" do código municipal da receita para o IBGE está presente.
assert_that(file.exists("tab_auxiliares/CodMunicRFB.csv"), 
            msg = "O arquivo de 'CodMunicRFB.csv' não está disponível")

# Baixa dados do controle de atualização
ControleAtualizacao <- read_xlsx("data/dataset/ControleAtualizacaoOSC.xlsx", 
                                 sheet = "ControleAtualizacao")

# Processos da Atualização
ProcessosAtualizacao <- read_xlsx("data/dataset/ControleAtualizacaoOSC.xlsx", 
                                  sheet = "ProcessosAtualizacao")

# Backups Gerados
BackupsFiles <- read_xlsx("data/dataset/ControleAtualizacaoOSC.xlsx", 
                          sheet = "BackupsFiles") %>% 
  # Evita problemas de consistência
  mutate(ControleAt_Id = as.character(ControleAt_Id), 
         FileFolder = as.character(FileFolder), 
         FileName = as.character(FileName), 
         FileSizeMB = as.numeric(FileSizeMB))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Controle das Atualizações ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

message("Controle das Atualizações")

# Caso estqa for a primeira atualização
if(nrow(ControleAtualizacao) == 0) {
  Att_Atual <- tibble(At_id = 1,
                      At_CodRef = NA,
                      At_Situacao = "Iniciada", 
                      At_DataRef = today(), 
                      At_DataInicio = now(), 
                      At_ValidadeDias = ValidadeAtts,
                      At_DtFimValidade = today() + days(ValidadeAtts))
  
}

# Caso não for a primeira atualização
if(nrow(ControleAtualizacao) > 0) {
  # Controle da atualização atual
  UltimaSituacao <- ControleAtualizacao %>% 
    dplyr::filter(At_DataInicio == max(At_DataInicio)) %>% 
    slice(1) %>% 
    select(At_Situacao) %>% unlist() %>% as.character() 
  
  if(UltimaSituacao == "Finalizada") {
    # Se a última atualização estiver finalizada, criar uma nova
    # base que no final será adiconada em ControleAtualizacao
    Att_Atual <- tibble(At_id = max(ControleAtualizacao$At_id) + 1,
                        At_CodRef = NA,
                        At_Situacao = "Iniciada", 
                        At_DataRef = today(), 
                        At_DataInicio = now(), 
                        At_ValidadeDias = ValidadeAtts, 
                        At_DtFimValidade = today() + days(ValidadeAtts))
    
  } else {
    # Caso a última atualização já tiver sido iniciada, mas não finalizada,
    # usar a última linha de ControleAtualizacao
    Att_Atual <- ControleAtualizacao %>% 
      dplyr::filter(At_DataInicio == max(At_DataInicio)) %>% 
      slice(1)
  }
  rm(UltimaSituacao)
}

# Controle dos processo da atualização atual
ProcessosAtt_Atual <- ProcessosAtualizacao %>% 
  # Seleciona apenas os processos da seleção atual
  dplyr::filter(At_id == Att_Atual$At_id[1]) %>% 
  # Cria uma variável que une o processo e se está completo ou não
  mutate(Controle = as.character(paste0(Processo_id, Completo))) %>% 
  # Ordena os processos por data
  arrange(desc(DataInicio)) %>% 
  # Garante consistência nos tipos de variáveis
  mutate(ControleAt_Id = as.character(ControleAt_Id), 
         At_id = as.integer(At_id),
         Data = as.Date(Data),
         Processo_id = as.integer(Processo_id),
         Processo_Nome = as.character(Processo_Nome),
         Completo = as.integer(Completo),
         DataInicio = as_datetime(DataInicio),
         DataFim = as_datetime(DataInicio),
         Controle = as.character(Controle))

# Se não existir um folder de atualização, criar um...
# "51": Processo 5 (Criação do diretório Backup) e 1 (completo)
if(!"51" %in% ProcessosAtt_Atual$Controle) {
  
  # Marca início do processo
  DataProcessoInicio <- now()
  
  # Loop para criar um diretório
  for (i in 1:99) {
    # i <- 1
    DirName <- paste0("backup_files/",
                      year(today()), "_", 
                      str_pad(i, 2, pad = "0"), "/")
    
    if(!dir.exists(DirName)) {
      
      # Cria novo diretório
      dir.create(DirName)
      
      # usa diretório dos backups para criar o código da atualização
      Att_Atual$At_CodRef <- DirName %>% 
        str_remove(DirBackupFiles) %>% 
        str_remove("/")
      
      break # Interrompe Loop
    }
    
    # Se o diretório não conseguir ser criado, stop!
    if (i == 99) {
      stop("Excesso de tentativas de criação de diretório backup")
    }
  }
  rm(i)
  
  # Atualiza controle de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_5"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 5,
            Processo_Nome = "baixar bases de dados brutas SRF",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "51")
  
  rm(DataProcessoInicio)
}

# Diretório de backup
DirName <- paste0("backup_files/", Att_Atual$At_CodRef[1], "/")
assert_that(dir.exists(DirName), 
            msg = "Diretório de Backup não existe")

rm(ValidadeAtts)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Carrega Dados da RFB ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Baixa os dados da Receita, se não tiver sido feito ainda
# "11": Processo 1 (baixar bases de dados brutas RFB) e 1 (completo)
if(!"11" %in% ProcessosAtt_Atual$Controle) {
  
  message("Carrega Dados da RFB")
  
  # Marca início do processo
  DataProcessoInicio <- now()
  
  library(DBI)
  library(RODBC)
  library(RPostgres)
  
  # Baixa dados da Receita Federal no Posgres do IPEA.
  
  # Baixa a chave secreta do código
  keys <- jsonlite::read_json("keys/rais_2019_key.json")
  
  # Verifica se pode condenar
  CanConnect <- dbCanConnect(RPostgres::Postgres(), 
                             dbname = keys$dbname,
                             host = keys$host,
                             port = keys$port,
                             user = keys$username, 
                             password = keys$password,
                             options="-c search_path=rfb_2023")
  
  assert_that(CanConnect, 
              msg = "Teste de coneção a 'rais_2019' falhou.")
  
  # conencta à base
  connec <- dbConnect(RPostgres::Postgres(), 
                      dbname = keys$dbname,
                      host = keys$host,
                      port = keys$port,
                      user = keys$username, 
                      password = keys$password,
                      options="-c search_path=rfb_2023")
  
  rm(keys)
  assert_that(dbIsValid(connec))
  
  # Query para buscar as informações nas tabelas "tb_rfb_empresas" e 
  # "tb_rfb_estabelecimentos". Já faz o Join entre as tabelas e também 
  # filtra pela natureza jurídica das organizações sem fins lucrativos
  # ('3069', '3220', '3301', '3999')
  
  # Filtra naturezas jurídicas:
  # grupo 3: Entidades sem fins lucrativos
  ## 306-9	Fundação Privada
  ## 322-0	Organização Religiosa
  ## 330-1	Organização Social (OS)
  ## 399-9	Associação Privada
  # Fonte: https://concla.ibge.gov.br/estrutura/natjur-estrutura/natureza-juridica-2021
  
  tb_JoinOSC <- dbGetQuery(connec, 
                           paste0("SELECT * FROM tb_rfb_empresas", 
                                  " RIGHT JOIN tb_rfb_estabelecimentos ", 
                                  "ON tb_rfb_estabelecimentos.cnpj_basico ",
                                  "= tb_rfb_empresas.cnpj_basico", 
                                  " WHERE natureza_juridica ",
                                  "IN ('3069', '3220', '3301', '3999')",
                                  # " LIMIT 1000", 
                                  ";"))
  
  # Debug:
  # tb_JoinOSC <- readRDS("backup_files/2023_01/intermediate_files/tb_JoinOSC.RDS")
  
  # Desconecta da base
  dbDisconnect(connec)
  
  rm(connec, CanConnect)
  
  # Caminho do arquivo Backup
  PathFile <- paste0(DirName, "tb_JoinOSC.RDS")
  
  # Salva Backup
  saveRDS(tb_JoinOSC, PathFile)
  
  # Atualiza realização de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_1"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 1,
            Processo_Nome = "baixar bases de dados brutas SRF",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "11")
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_1"), 
            FileFolder = DirName, 
            FileName = "tb_JoinOSC.RDS", 
            FileSizeMB = file.size(paste0(DirName, "tb_JoinOSC.RDS"))/1024000)
  
  rm(PathFile, DataProcessoInicio)
} else {message("Dados da RFB já carregados anteriormente")}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identificação OSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Executa o processo se não foi feito
# "21": Processo 2 (Identificação OSC via Razão Social) e 1 (completo)
if(!"21" %in% ProcessosAtt_Atual$Controle) {
  
  message("Identificação OSC via Razão Social")
  
  # Marca início do processo
  DataProcessoInicio <- now()
  
  # Se o arquivo não estiver carregado, carrega ele.
  if(!exists("tb_JoinOSC")) {
    
    PathFile <- paste0(DirName, "intermediate_files/tb_JoinOSC.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(PathFile), 
                msg = paste0("Arquivo '", PathFile, "' não encontrado."))
    
    # Carrega o arquivo com os dados da busca SQL base SRF
    tb_JoinOSC <- readRDS(PathFile)
    # names(tb_JoinOSC)
    # nrow(tb_JoinOSC)
    rm(PathFile)
  }
  
  # Filtra por CNAE
  # 8550301	- Administração de caixas escolares
  # 94201 - Atividades de organizações sindicais
  # 8112500 - Condomínios prediais
  # 9492800 (em conjunto com certos nomes de razão social) - Atividades de organizações políticas
  # Fonte: https://concla.ibge.gov.br/busca-online-cnae.html?view=estrutura
  
  # Expressões regulares que indicam grupos partidários:
  IndicadoresPartidos <- paste(c("^COLIGACAO", "^COLIGAO", "^COMISSAO EXECUTIVA", 
                                 "^COMISSAO MUNICIPAL", "^COMISSAO PROVISORIA", 
                                 "^COMITE DA COLIGACAO", "^COMITE DE GASTOS E PROPAGANDA", 
                                 "^DIRETORIO REG", "^DIRETORIO TRAB", "^COMITE DE PROPAGANDA", 
                                 "^COMITE FINAANCEIRO", "^DIRETORIO TRABALHISTA", 
                                 "^DIRETORIO REGIONAL", "^DIRETORIO EXECUTIVO", 
                                 "^DIRETORIO DA FRENTE LIBERAL", 
                                 "^DIRETORIO DA SOCIAL DEMOCRACIA", 
                                 "^DIRETORIO DA COLIGACAO", "^DIRETORIO DA MUNICIPAL DA FRENTE", 
                                 "^DIRETORIO LIBERAL", "^DIRETORIO PROGRESSISTA", "^DORETORIO MUNICIPAL", 
                                 "MUNIC DO PART DA FREN"), collapse = "|")
  
  # Descobre OSC com base no CNAE e expressões regulares partidárias
  tb_JoinOSC <- tb_JoinOSC %>% 
    mutate(IsOSC = case_when(cnae_fiscal_principal %in% c("8112500", "8550301") ~ FALSE, 
                             str_sub(cnae_fiscal_principal, 1, 5) == "94201" ~ FALSE,
                             str_detect(razao_social, IndicadoresPartidos) &
                               cnae_fiscal_principal == "9492800" ~ FALSE, 
                             TRUE ~ TRUE))
  
  rm(IndicadoresPartidos)
  
  # Usa função find_OSC para determinar se um estabelecimento é OSC:
  
  # Expressões usadas em findosc
  NonOSCNames <- fread("data/dataset/NonOSCNames.csv") %>% 
    as_tibble()
  # names(NonOSCNames)
  
  # Uso da função find_OSC:
  source("src/findosc-v2023.R")
  tb_JoinOSC$IsOSC <- tb_JoinOSC$IsOSC & 
    find_OSC(tb_JoinOSC$razao_social, NonOSCNames, verbose = FALSE)
  
  rm(NonOSCNames, find_OSC)
  
  #  Inserindo OSCs que estavam na última versão do Banco 
  # (princípio de não deletar OSC do banco exclusivamente pelo find_OSC)
  # bkp <- tb_JoinOSC
  idControl <- readRDS("tab_auxiliares/idControl.RDS")
  
  # Somente trazer para o banco novo OSCs ativas na última versão
  idControl_ativa <- idControl[idControl$bo_osc_ativa, ]

  tb_JoinOSC <- tb_JoinOSC %>% 
    # fonte da identificação da OSC
    mutate(ft_IsOSC = ifelse(IsOSC, paste0("findOSC.R_", Att_Atual$At_CodRef[1]), 
                                      NA), 
           IsOSC = ifelse(cnpj %in% idControl_ativa$cd_identificador_osc, 
                          TRUE, IsOSC), 
           # Marca quem foi adicionado pelo legado do passado.
           ft_IsOSC = ifelse(IsOSC & is.na(ft_IsOSC), "findOSC_legado", ft_IsOSC))
  # table(tb_JoinOSC2[["ft_IsOSC"]], useNA = "always")
  rm(idControl, idControl_ativa)
  
  # Muda nome do objeto para marcar mudança de processamento:
  Tb_OSC_Full <- tb_JoinOSC %>% 
    # Mantem apenas OSCs ativas no banco:
    mutate(situacao = as.integer(situacao_cadastral), 
           bo_osc_ativa = situacao %in% c(2, 3, 4)) %>%
    dplyr::filter(IsOSC, bo_osc_ativa)
  
  # Salva Backup
  PathFile <- paste0(DirName, "intermediate_files/Tb_OSC_Full.RDS")
  
  saveRDS(Tb_OSC_Full, PathFile)
  
  # Atualiza realização de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_2"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 2,
            Processo_Nome = "Identificação OSC",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "21")
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_2"), 
            FileFolder = str_remove(PathFile, "Tb_OSC_Full.RDS$"), 
            FileName = "Tb_OSC_Full.RDS", 
            FileSizeMB = file.size(PathFile)/1024000)
  
  rm(DataProcessoInicio, PathFile)
  rm(tb_JoinOSC) # não vamos mais utilizar esses dados
} else {message("Identificação OSC via Razão Social já feita anteriormente")}



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Determinação das áreas de atuação OSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Executa o processo se não foi feito
# "31": Processo 2 (Determinação das áreas de atuação OSC) e 1 (completo)
if(!"31" %in% ProcessosAtt_Atual$Controle) {
  
  message("Determinação das áreas de atuação OSC")
  
  # Marca início do processo
  DataProcessoInicio <- now()
  
  # Se o arquivo não estiver carregado, carrega ele.
  if(!(exists("Tb_OSC_Full") && "data.frame" %in% class(Tb_OSC_Full)) ) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    PathFile <- paste0(DirName, "intermediate_files/Tb_OSC_Full.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(PathFile), 
                msg = paste0("Arquivo '", PathFile, "' não encontrado."))
    
    # Carrega ele
    Tb_OSC_Full <- readRDS(PathFile)
    # names(Tb_OSC_Full)
    # nrow(Tb_OSC_Full)
    rm(PathFile)
  }
  
  # Regras para determinar as subáreas de atuação
  DB_SubAreaRegras <- fread("data/dataset/IndicadoresAreaAtuacaoOSC.csv",
                            encoding = "Latin-1")
  
  # Relação entre micro áreas e macro áreas
  DB_AreaSubaria <- fread("data/dataset/Areas&Subareas.csv",
                          encoding = "Latin-1")
  
  # Função para determinar as áreas de atuação
  source("src/specifcFuntions/AreaAtuacaoOSC.R")
  
  # Transforma Tb_OSC_Full em DB_OSC
  DB_OSC <- Tb_OSC_Full %>%
    rename(cnae = cnae_fiscal_principal) %>% 
    mutate(micro_area_atuacao = NA)
  
  rm(Tb_OSC_Full) # não vamos mais utilizar esses dados
  
  # Usa função "AreaAtuacaoOSC" para determinar qual a área de atuação
  # das OSCs
  DB_OSC$micro_area_atuacao <- AreaAtuacaoOSC(select(DB_OSC, 
                                                     cnpj_basico, 
                                                     razao_social, 
                                                     cnae, 
                                                     micro_area_atuacao), 
                                              DB_SubAreaRegras, 
                                              chuck_size = 10000, verbose = FALSE)
  
  
  DB_OSC <- DB_OSC %>% 
    # Se não foi indentificado pelo sistema, colocar "Outras"
    mutate(micro_area_atuacao = ifelse(is.na(micro_area_atuacao), 
                                       "Outras organizações da sociedade civil", 
                                       micro_area_atuacao)) %>% 
    # Insere Macro áreas de atuação
    left_join(DB_AreaSubaria, by = "micro_area_atuacao")
    
  
  rm(AreaAtuacaoOSC, DB_SubAreaRegras, DB_AreaSubaria)

  # Salva Backup
  PathFile <- paste0(DirName, "intermediate_files/DB_OSC.RDS")
  saveRDS(DB_OSC, PathFile)

  # Atualiza realização de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_3"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 3,
            Processo_Nome = "Determinação das áreas de atuação OSC",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "31")
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_3"), 
            FileFolder = str_remove(PathFile, "DB_OSC.RDS$"), 
            FileName = "DB_OSC.RDS", 
            FileSizeMB = file.size(PathFile)/1024000)
  
  rm(DataProcessoInicio, PathFile)
  # ls()
} else {message("Determinação das áreas de atuação OSC já feita anteriormente")}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Desmembramento da base RFB ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Desmembra todos os dados da Receita Federal nas tabelas específicas 
# do MOSC

# Executa o processo se não foi feito anteriormente
# "61": Processo 6 (Desmembramento da base RFB) e 1 (completo)
if(!"61" %in% ProcessosAtt_Atual$Controle) {
  
  message("Determinação das áreas de atuação OSC")
  
  assert_that(file.exists(paste0(DirName, "intermediate_files/GalileoINPUT.RDS")),
              msg = "Arquivo de localização do Galileo não está presente")
  
  # Marca início do processo
  DataProcessoInicio <- now()
  
  # Se o arquivo não estiver carregado, carrega ele.
  if(!(exists("DB_OSC") && "data.frame" %in% class(DB_OSC)) ) {
    
    # Se o arquivo já tiver sido baixado, vamos direto para carregar ele.
    PathFile <- paste0(DirName, "intermediate_files/DB_OSC.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(PathFile), 
                msg = paste0("Arquivo '", PathFile, "' não encontrado."))
    
    # Carrega ele
    DB_OSC <- readRDS(PathFile)
    # names(DB_OSC)
    # nrow(DB_OSC)
    rm(PathFile)
  }
  
  # Código da fonte de dados RFB
  FonteRFB <- paste0("CNPJ/SRF/MF/", Att_Atual$At_CodRef[1])
  
  # Formata o CNPJ, usado aqui como chave-primária das OSC:
  DB_OSC <- DB_OSC %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cnpj), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  #  Inserir protocolo do idControl:
  idControl <- readRDS("tab_auxiliares/idControl.RDS") %>% 
    select(id_osc, cd_identificador_osc)
  
  
  ## Tabela: tb_osc ####
  
  # Resgata o "id_osc" antigo
  
  # Formata base:
  tb_osc <- DB_OSC %>% 
    rename(tx_apelido_osc = nome_fantasia, 
           cd_situacao_cadastral = situacao) %>% 
    
    # Resgata o id_osc antigo.
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(ft_apelido_osc = FonteRFB, 
           ft_identificador_osc = FonteRFB,
           ft_osc_ativa = FonteRFB, 
           bo_osc_ativa = TRUE, 
           bo_Filial = identificador_matrizfilial == "2", 
           ft_Filial = FonteRFB) %>% 
    
    arrange(id_osc, data_de_inicio_atividade) %>% 
    select(id_osc, 
           # ft_IsOSC, # rodar etapa anterior para inserir essa variável
           tx_apelido_osc, ft_apelido_osc, cd_identificador_osc,
           ft_identificador_osc, bo_osc_ativa, ft_osc_ativa, bo_Filial,
           ft_Filial, cd_situacao_cadastral) %>% 
    # Evitar dar fonte de dado missing:
    mutate(ft_apelido_osc = ifelse(is.na(tx_apelido_osc), NA, ft_apelido_osc),
           ft_identificador_osc = ifelse(is.na(cd_identificador_osc), NA, ft_identificador_osc),
           ft_osc_ativa = ifelse(is.na(bo_osc_ativa), NA, ft_osc_ativa), 
           ft_Filial = ifelse(is.na(bo_Filial), NA, ft_Filial))
  
  # Cria id_osc dos novos CNPJs:
  
  if(sum(is.na(tb_osc$id_osc)) > 0) {
    # id máximo antigo
    Max_OldID <- max(tb_osc$id_osc, na.rm = TRUE)
    
    # Novos IDs
    NewID <- seq(from = Max_OldID + 1, 
                 to = Max_OldID + sum(is.na(tb_osc$id_osc)), 
                 by = 1)
    
    # Adiciona novos IDs
    tb_osc$id_osc[is.na(tb_osc$id_osc)] <- NewID
    
    rm(Max_OldID, NewID)
    
    # Guarda os novos IDs em idControl
    NewId <- tb_osc %>% 
      distinct(id_osc, cd_identificador_osc) %>% 
      select(everything())
    
    # Update idControl
    idControl <- bind_rows(idControl, NewId) %>% 
      distinct(id_osc, cd_identificador_osc) %>% 
      select(everything())
    
    saveRDS(idControl, "tab_auxiliares/idControl.RDS")
    rm(NewId)
  }
  
  # Garante chaves primárias únicas
  assert_that(nrow(tb_osc) == length(unique(tb_osc$id_osc)) && 
                sum(is.na(tb_osc$id_osc)) == 0, 
              msg = "Chaves primárias em tb_osc não únicas.")
  
  # Garante chaves primárias não nulas
  assert_that(sum(is.na(tb_osc$id_osc)) == 0 , 
              msg = "Criação de nulos em chaves primárias em tb_osc.")
  
  # Por enquanto, vou anular os valores de tx_apelido_osc, até entender
  # melhor essa variável
  tb_osc[["tx_apelido_osc"]] <- NA_character_
  tb_osc[["ft_apelido_osc"]] <- NA_character_
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_osc[["cd_identificador_osc"]] <- as.numeric(tb_osc[["cd_identificador_osc"]])
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_osc.RDS")
  saveRDS(tb_osc, PathFile)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = str_remove(PathFile, "tb_osc.RDS$"), 
            FileName = "tb_osc.RDS", 
            FileSizeMB = file.size(PathFile)/1024000 )

  
  # Tabela: tb_dados_gerais ####
  
  tb_dados_gerais <- DB_OSC %>% 
    rename(cd_natureza_juridica_osc = natureza_juridica, 
           tx_razao_social_osc = razao_social, 
           tx_nome_fantasia_osc = nome_fantasia) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(dt_fundacao_osc = as.character(ymd(data_de_inicio_atividade)),
           dt_ano_cadastro_cnpj = as.character(ymd(data_de_inicio_atividade)),
           cd_classe_atividade_economica_osc = str_sub(cnae, 1, 5), # verificar se é isso mesmo!
           ft_natureza_juridica_osc = FonteRFB, 
           ft_razao_social_osc = FonteRFB, 
           ft_nome_fantasia_osc = FonteRFB, 
           ft_fundacao_osc = FonteRFB,
           ft_ano_cadastro_cnpj = FonteRFB,
           ft_classe_atividade_economica_osc = FonteRFB,
           
           # Evitar dar fonte de dado missing:
           ft_natureza_juridica_osc = ifelse(is.na(cd_natureza_juridica_osc), NA, ft_natureza_juridica_osc),
           ft_razao_social_osc = ifelse(is.na(tx_razao_social_osc), NA, ft_razao_social_osc),
           ft_nome_fantasia_osc = ifelse(is.na(tx_nome_fantasia_osc), NA, ft_nome_fantasia_osc),
           ft_fundacao_osc = ifelse(is.na(dt_fundacao_osc), NA, ft_fundacao_osc),
           ft_ano_cadastro_cnpj = ifelse(is.na(dt_ano_cadastro_cnpj), NA, ft_ano_cadastro_cnpj),
           ft_classe_atividade_economica_osc = ifelse(is.na(cd_classe_atividade_economica_osc), NA, ft_classe_atividade_economica_osc)
           ) %>% 
    
    select(id_osc, cd_identificador_osc, cd_natureza_juridica_osc, ft_natureza_juridica_osc, 
           tx_razao_social_osc, ft_razao_social_osc, tx_nome_fantasia_osc, 
           ft_nome_fantasia_osc, dt_fundacao_osc, ft_fundacao_osc, 
           dt_ano_cadastro_cnpj, ft_ano_cadastro_cnpj, 
           cd_classe_atividade_economica_osc, ft_classe_atividade_economica_osc, 
           situacao)
  
  
  assert_that(sum(is.na(tb_dados_gerais$id_osc)) == 0)
  assert_that(nrow(tb_dados_gerais) == length(unique(tb_dados_gerais$id_osc)))
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_dados_gerais[["cd_identificador_osc"]] <- as.numeric(tb_dados_gerais[["cd_identificador_osc"]])
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_dados_gerais.RDS")
  saveRDS(tb_dados_gerais, PathFile)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = str_remove(PathFile, "tb_dados_gerais.RDS$"), 
            FileName = "tb_dados_gerais.RDS", 
            FileSizeMB = file.size(PathFile)/1024000 )

  # Tabela: tb_contato ####
  
  tb_contato <- DB_OSC %>% 
    rename(tx_email = correio_eletronico) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(tx_telefone = ifelse(is.na(telefone_1), NA, 
                                paste0(ddd_1, " ", telefone_1)),
           ft_telefone = FonteRFB, 
           ft_email = FonteRFB,
           bo_nao_possui_email = is.na(tx_email)) %>% 
    select(id_osc, tx_telefone, ft_telefone, tx_email, ft_email,
           bo_nao_possui_email) %>% 
    # Evita linhas que não tem informação.
    dplyr::filter(!(is.na(tx_email) & is.na(tx_telefone))) %>% 
    # Evitar dar fonte de dado missing:
    mutate(ft_telefone = ifelse(is.na(tx_telefone), NA, ft_telefone),
           ft_email = ifelse(is.na(tx_email), NA, ft_email))
  
  # Colocar contato no formato curto.
  
  tb_contato2 <- DB_OSC %>% 
    rename(tx_email = correio_eletronico) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    mutate(tx_telefone = ifelse(is.na(telefone_1), NA, 
                                paste0(ddd_1, " ", telefone_1))) %>% 
    
    select(id_osc, tx_telefone, tx_email) %>% 
    gather(key = tx_tipo_contato, value = tx_contato, tx_telefone, tx_email) %>% 
    arrange(id_osc) %>% 
    mutate(ft_contato = FonteRFB, 
           tx_tipo_contato = str_remove(tx_tipo_contato, "tx_")) %>% 
    
    # Evita linhas que não tem informação.
    dplyr::filter(!is.na(tx_contato))
  
  assert_that(sum(is.na(tb_contato$id_osc)) == 0)
  assert_that(nrow(tb_contato) == length(unique(tb_contato$id_osc)))
  
  assert_that(sum(is.na(tb_contato2$id_osc)) == 0)
  assert_that(nrow(tb_contato2) == 
                length(unique(paste(tb_contato2$id_osc, tb_contato2$tx_tipo_contato))))
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_contato.RDS")
  saveRDS(tb_contato, PathFile)
  
  PathFile <- paste0(DirName, "output_files/tb_contato2.RDS")
  saveRDS(tb_contato2, PathFile)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_contato.RDS", 
            FileSizeMB = file.size(paste0(DirName, "output_files/tb_contato.RDS"))/1024000 ) %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_contato2.RDS", 
            FileSizeMB = file.size(paste0(DirName, "output_files/tb_contato2.RDS"))/1024000 )
  
  # Tabela: tb_localizacao ####

  # Dados de geolocalização:
  Galileo_data <- readRDS(paste0(DirName, 
                                 "intermediate_files/GalileoINPUT.RDS"))
  
  # Tabela com o de/para do código municipal da Receita para o
  # código IBGE.
  CodMunicRFB <- fread("tab_auxiliares/CodMunicRFB.csv", 
                       encoding = "Latin-1") %>% 
    # Formata campos
    mutate(CodMuniRFB = str_pad(as.character(CodMuniRFB), 
                                width = 4, 
                                side = "left", 
                                pad = "0"),
           CodMunicIBGE = str_pad(as.character(CodMunicIBGE), 
                                  width = 7, 
                                  side = "left", 
                                  pad = "0"))
  
  # Formata dados de localização das OSCs
  tb_localizacao <- DB_OSC %>% 
    
    rename(nr_localizacao = numero,
           tx_endereco_complemento = complemento,
           tx_bairro = bairro,
           CodMuniRFB = municipio,
           nr_cep = cep) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Incorpora dados de geolocalização
    left_join(Galileo_data, by = "cd_identificador_osc") %>% 
    
    # Não pode ter sede no exterior nem valor nulo de município
    dplyr::filter(CodMuniRFB != "9707", 
                  !is.na(CodMuniRFB)) %>% 
    
    # Coloca código municipal do IBGE
    left_join(select(CodMunicRFB, CodMuniRFB, CodMunicIBGE), 
              by = ("CodMuniRFB")) %>% 
    
    rename(tx_longitude = Longitude, 
           tx_latitude = Latitude, 
           cd_municipio = CodMunicIBGE) %>% 
    mutate(tx_endereco = paste0(tipo_de_logradouro, " ", logradouro),
           ft_endereco = FonteRFB,
           ft_localizacao = FonteRFB,
           ft_endereco_complemento = FonteRFB,
           ft_bairro = FonteRFB,
           ft_municipio = FonteRFB,
           ft_cep = FonteRFB,
           ft_latlon = "Software Galileo",
           bo_oficial = TRUE) %>% 
    select(cd_identificador_osc, id_osc, tx_latitude, tx_longitude,
           cd_precisao_localizacao, ft_latlon, tx_endereco, ft_endereco,
           nr_localizacao, ft_localizacao, tx_endereco_complemento,
           ft_endereco_complemento, tx_bairro, ft_bairro, cd_municipio,
           ft_municipio, nr_cep, ft_cep, bo_oficial) %>% 
    # Evitar dar fonte de dado missing:
    mutate(ft_endereco = ifelse(is.na(tx_endereco), NA, ft_endereco),
           ft_localizacao = ifelse(is.na(nr_localizacao), NA, ft_localizacao),
           ft_endereco_complemento = ifelse(is.na(tx_endereco_complemento), NA, ft_endereco_complemento),
           ft_bairro = ifelse(is.na(tx_bairro), NA, ft_bairro),
           ft_municipio = ifelse(is.na(cd_municipio), NA, ft_municipio),
           ft_latlon = ifelse(is.na(tx_latitude) | is.na(tx_longitude), NA, ft_latlon),
           ft_cep = ifelse(is.na(nr_cep), NA, ft_cep))
  
  # Como o padrão da base de dados é class("cd_identificador_osc") == numeric , 
  # vou deixar assim
  tb_localizacao[["cd_identificador_osc"]] <- as.numeric(tb_localizacao[["cd_identificador_osc"]])
  tb_localizacao[["cd_municipio"]] <- as.numeric(tb_localizacao[["cd_municipio"]])
  tb_localizacao[["nr_cep"]] <- as.numeric(tb_localizacao[["nr_cep"]])
  
  # Restrições de "cd_municipio": não pode ter nulo (nem 0)
  assert_that(sum(is.na(tb_localizacao[["cd_municipio"]])) == 0, 
              msg = "Valores nulos encontrados em 'cd_municipio'")
  
  assert_that(sum(tb_localizacao[["cd_municipio"]] == 0) == 0, 
              msg = "Valores nulos encontrados em 'cd_municipio'")
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_localizacao.RDS")
  saveRDS(tb_localizacao, PathFile)
  rm(Galileo_data, CodMunicRFB)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_localizacao.RDS", 
            FileSizeMB = file.size(PathFile)/1024000 )
  
  
  
  
  # Tabela: tb_area_atuacao ####
  
  tb_area_atuacao <- DB_OSC %>% 
    dplyr::filter(!is.na(micro_area_atuacao) & !is.na(macro_area_atuacao)) %>% 
    rename(tx_area_atuacao = macro_area_atuacao, 
           tx_subarea_atuacao = micro_area_atuacao) %>% 
    
    # Insere "id_osc"
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Nome da fonte da área de atuação receita
    mutate(ft_area_atuacao = paste0("AreaAtuacaoOSC.R_", 
                                    Att_Atual$At_CodRef[1]), 
           ft_area_atuacaoPadronizado = "CNPJ/RFB",
           bo_oficial = TRUE) %>% 
    
    select(id_osc, cd_identificador_osc, tx_area_atuacao, 
           tx_subarea_atuacao, ft_area_atuacao, bo_oficial, 
           ft_area_atuacaoPadronizado) 
  
  # Identifica área de atuação via CNES/MS
  if(file.exists("data/raw/MS/InputCNES.RDS")) {
    InputCNES <- readRDS("data/raw/MS/InputCNES.RDS")
    
    # names(InputCNES)
    
    # Usa CEBAS/MS para identificar OSC como da área da saúde
    newRows <- InputCNES %>% 
      rename(cd_identificador_osc = cpf_cnpj) %>% 
      
      # Insere "id_osc"
      left_join(idControl, by = "cd_identificador_osc") %>% 
      
      dplyr::filter(!is.na(id_osc)) %>% 
      
      # Evita duplicação dos campos
      distinct(id_osc, .keep_all = TRUE) %>% 
      
      mutate(tx_area_atuacao = "Saúde", 
             ft_area_atuacao = paste0("CNES/MS/", Att_Atual$At_CodRef[1]), 
             ft_area_atuacaoPadronizado = "CNES/MS",
             tx_subarea_atuacao = NA, 
             bo_oficial = TRUE) %>% 
      select(id_osc, cd_identificador_osc, tx_area_atuacao, 
             tx_subarea_atuacao, ft_area_atuacao, bo_oficial, 
             ft_area_atuacaoPadronizado)
    
    tb_area_atuacao <- tb_area_atuacao %>% 
      bind_rows(newRows) %>% 
      arrange(id_osc)
    
    # table(tb_area_atuacao$ft_area_atuacao)
    rm(InputCNES, newRows)
  }
  
  # Identifica área de atuação via CEBAS/MS
  if(file.exists("data/raw/MS/InputCEBAS.xlsx")) {
    InputCEBAS <- read_xlsx("data/raw/MS/InputCEBAS.xlsx", sheet = 1)
    
    # Usa CEBAS/MS para identificar OSC como da área da saúde
    newRows <- InputCEBAS %>% 
      rename(cd_identificador_osc = NU_CNPJ) %>% 
      
      # Insere "id_osc"
      left_join(idControl, by = "cd_identificador_osc") %>% 
      
      dplyr::filter(!is.na(id_osc)) %>%
      
      # Evita duplicação dos campos
      distinct(id_osc, .keep_all = TRUE) %>% 
      
      mutate(tx_area_atuacao = "Saúde", 
             ft_area_atuacao = paste0("CEBAS/MS/", Att_Atual$At_CodRef[1]), 
             ft_area_atuacaoPadronizado = "CEBAS/MS",
             tx_subarea_atuacao = NA, 
             bo_oficial = TRUE) %>% 
        select(id_osc, cd_identificador_osc, tx_area_atuacao, 
               tx_subarea_atuacao, ft_area_atuacao, bo_oficial, 
               ft_area_atuacaoPadronizado)
    
    tb_area_atuacao <- tb_area_atuacao %>% 
      bind_rows(newRows) %>% 
      arrange(id_osc)
    
    # table(tb_area_atuacao$ft_area_atuacao)
    rm(InputCEBAS, newRows)
  }
  
  # Identifica área de atuação via CNEAS/MDS
  if(file.exists("data/raw/MDS/InputCNEAS.xlsx")) {
    InputCNEAS <- read_xlsx("data/raw/MDS/InputCNEAS.xlsx", sheet = 1)
    
    # Formata base InputCNEAS
    names(InputCNEAS)[1] <- "cnpj"
    InputCNEAS <- InputCNEAS %>% 
      mutate(cnpj = str_remove_all(cnpj, fixed(".")), 
             cnpj = str_remove_all(cnpj, fixed("/")), 
             cnpj = str_remove_all(cnpj, fixed("-")))
    
    # Usa CNEAS/MDS para identificar OSC como assistência social
    newRows <- InputCNEAS %>% 
      rename(cd_identificador_osc = cnpj) %>% 
      
      # Insere "id_osc"
      left_join(idControl, by = "cd_identificador_osc") %>% 
      
      dplyr::filter(!is.na(id_osc)) %>% 
      
      # Evita duplicação dos campos
      distinct(id_osc, .keep_all = TRUE) %>% 
      
      mutate(tx_area_atuacao = "Assistência social", 
             ft_area_atuacao = paste0("CNEAS/MDS/", Att_Atual$At_CodRef[1]), 
             ft_area_atuacaoPadronizado = "CNEAS/MDS",
             tx_subarea_atuacao = NA, 
             bo_oficial = TRUE) %>% 
      select(id_osc, cd_identificador_osc, tx_area_atuacao, 
             tx_subarea_atuacao, ft_area_atuacao, bo_oficial, 
             ft_area_atuacaoPadronizado)
    
    tb_area_atuacao <- tb_area_atuacao %>% 
      bind_rows(newRows) %>% 
      arrange(id_osc)
    
    # table(tb_area_atuacao$ft_area_atuacao)
    rm(InputCNEAS, newRows)
  }
 
  # Insere os códigos das áreas
  
  ## Tabelas auxiliares:
  dc_area_atuacao <- fread("tab_auxiliares/dc_area_atuacao.csv", encoding = "Latin-1")
  dc_subarea_atuacao <- fread("tab_auxiliares/dc_subarea_atuacao.csv", encoding = "Latin-1")
  
  tb_area_atuacao <- tb_area_atuacao %>% 
    left_join(dc_area_atuacao, by = "tx_area_atuacao") %>% 
    left_join(select(dc_subarea_atuacao, -cd_area_atuacao), 
              by = "tx_subarea_atuacao") %>% 
    select(id_osc, cd_area_atuacao, cd_subarea_atuacao,
           ft_area_atuacao, bo_oficial, 
           ft_area_atuacaoPadronizado)
  # table(tb_area_atuacao2$tx_area_atuacao[is.na(tb_area_atuacao2$cd_area_atuacao)])
  # table(tb_area_atuacao2$tx_subarea_atuacao[is.na(tb_area_atuacao2$cd_subarea_atuacao)])
  
  rm(dc_area_atuacao, dc_subarea_atuacao)
  
  # Adiciona novos IDs:
  
  # Estou aqui !!!! ####
  
  Control_Id_AreaAtuacao <- readRDS("tab_auxiliares/Control_Id_AreaAtuacao.RDS")
  
  # Tabela para iserir os Ids
  tb_area_atuacao2 <- tb_area_atuacao %>% 
    # Variáveis necessárias para o left_join com o controle de id
    mutate(has_area = !is.na(cd_area_atuacao), 
           has_subarea = !is.na(cd_subarea_atuacao)) %>% 
    # Insere os controle das últimas atualizações
    left_join(Control_Id_AreaAtuacao, 
              by = c("id_osc", "ft_area_atuacaoPadronizado",
                     "has_area", "has_subarea")) %>% 
    mutate(ft_area_atuacao = ifelse(is.na(id_area_atuacao), 
                                    NA, ft_area_atuacao)) %>% 
    select(everything())
  
  # Coloca novos IDs:
  if(sum(is.na(tb_area_atuacao2$id_area_atuacao)) > 0) {
    
    # Linhas que são missing:
    MissID <- is.na(tb_area_atuacao2$id_area_atuacao)
    
    # Cria novos Ids
    NewID <- seq_len(sum(MissID)) + max(Control_Id_AreaAtuacao$id_area_atuacao)
    
    # Insere novos Ids
    tb_area_atuacao2$id_area_atuacao[MissID] <- NewID
    
    # Update do controle de IDs
    idAreaAtuacaoControl_Up <-  tb_area_atuacao2 %>% 
      # Seleciona apenas as linhas com IDs novos
      add_column(JaPresente = MissID) %>% 
      dplyr::filter(JaPresente) %>% 
      # Apenas colunas do Controle ID
      select(id_area_atuacao, id_osc, ft_area_atuacaoPadronizado, 
             has_area, has_subarea) %>% 
      # Insere novas linhas
      bind_rows(Control_Id_AreaAtuacao) %>% 
      arrange(id_area_atuacao) %>% 
      select(everything())
    # sum(is.na(idAreaAtuacaoControl_Up$id_area_atuacao))
    
    # saveRDS(idAreaAtuacaoControl_Up, 
    #         "tab_auxiliares/idAreaAtuacaoControl.RDS")
    rm(idAreaAtuacaoControl_Up)
  }
  
  # Finaliza o banco
  tb_area_atuacao <- tb_area_atuacao %>% 
    # Adiciona as colunas
    add_column(id_area_atuacao = tb_area_atuacao2[["id_area_atuacao"]], 
               ft_area_atuacao2 = tb_area_atuacao2[["ft_area_atuacao"]]) %>% 
    # Garante que ft_area_atuacao será NA nos id novos
    mutate(ft_area_atuacao = ft_area_atuacao2) %>% 
  select(id_area_atuacao, id_osc, cd_area_atuacao, cd_subarea_atuacao,
         ft_area_atuacao, bo_oficial, ft_area_atuacaoPadronizado)
  
  rm(Control_Id_AreaAtuacao, tb_area_atuacao2)

  assert_that(sum(is.na(tb_area_atuacao$id_area_atuacao)) == 0, 
              msg = "Valores nulos da chave-primária de 'tb_area_atuacao'")
  
  assert_that(length(unique(tb_area_atuacao$id_area_atuacao)) == nrow(tb_area_atuacao), 
              msg = "Chave-primárias duplicadas em 'tb_area_atuacao'")
  
  # Salva Backup
  PathFile <- paste0(DirName, "output_files/tb_area_atuacao.RDS")
  saveRDS(tb_area_atuacao, PathFile)
  
  # Registra novo arquivo salvo
  BackupsFiles <- BackupsFiles %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            FileFolder = paste0(DirName, "output_files/"), 
            FileName = "tb_area_atuacao.RDS", 
            FileSizeMB = file.size(PathFile)/1024000 )
  
  
  # Atualiza realização de processos:
  ProcessosAtt_Atual <- ProcessosAtt_Atual %>% 
    add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_6"), 
            At_id = Att_Atual$At_id[1],
            Data = today(),
            Processo_id = 6,
            Processo_Nome = "Desmembramento da base RFB",
            Completo = 1,
            DataInicio = DataProcessoInicio,
            DataFim = now(),
            Controle = "61")
  
  rm(DataProcessoInicio, FonteRFB, PathFile)
  rm(tb_osc, tb_dados_gerais, tb_contato, tb_contato2, 
     tb_localizacao, tb_area_atuacao)
  rm(tb_area_atuacao2, tb_area_atuacao3)
} else {message("Desmembramento da base RFB já feito anteriormente")}


rm(DB_OSC)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Registra Controles de Atualização ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Criar critérios aqui para determinar que uma atualização foi concluida
# com sucesso.
AtualizacaoFinalizada <- FALSE

if(AtualizacaoFinalizada) {
  Att_Atual$At_DataFim[1] <- now()
  Att_Atual$At_Situacao[1] <- "Finalizada"
}
rm(AtualizacaoFinalizada)

# Atualiza processos desta última atualização
ProcessosAtualizacao <- ProcessosAtualizacao %>%
  # Filtro para evitar duplicação de linhas
  dplyr::filter(At_id != Att_Atual$At_id[1]) %>% 
  bind_rows(ProcessosAtt_Atual)

# Atualiza controle geral da atualização
ControleAtualizacao <- ControleAtualizacao %>% 
  # Filtro para evitar duplicação de linhas
  dplyr::filter(At_id != Att_Atual$At_id[1]) %>% 
  bind_rows(Att_Atual)

# Cria uma cópia desta rotina 
ScriptBackup <- paste0("OSC-v", str_remove_all(today(), "-"), ".R")

file.copy(from = ArquivoAtualizacao, 
          to = paste0(DirName, ScriptBackup) )

# Adiciona rotina criada na lista de backups
BackupsFiles <- BackupsFiles %>% 
  add_row(ControleAt_Id = paste0(Att_Atual$At_id[1], "_7"), 
          FileFolder = DirName, 
          FileName = ScriptBackup, 
          FileSizeMB = file.size(paste0(DirName, ScriptBackup))/1024000 )

rm(ScriptBackup)

# Salva arquivos
fwrite(ProcessosAtualizacao, "data/dataset/ProcessosAtualizacao.csv", sep = ";", dec = ",")
fwrite(ControleAtualizacao, "data/dataset/ControleAtualizacao.csv", sep = ";", dec = ",")
fwrite(BackupsFiles, "data/dataset/BackupsFiles.csv", sep = ";", dec = ",")

# Limpa memória
rm(ControleAtualizacao, Att_Atual)
rm(ProcessosAtualizacao, ProcessosAtt_Atual, BackupsFiles)
rm(DirName)
ls()

# Fim ####