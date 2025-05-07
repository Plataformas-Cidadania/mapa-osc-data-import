# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: as rotinas para separar organizações não lucrativas que
# conceitualmente não são OSC (cartórios, partidos, comissões de formatura etc),
# utilizando a função 'findosc-v2023' e outros procedimentos.

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2024-07-12

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes
# tb_JoinOSC (objeto na memódia ou arquivo em "{diretorio_att}intermediate_files")
# diretorio_att
# processos_att_atual
# Caso definicoes$att_teste == FALSE:
## conexao_mosc 
## tb_backups_files
## tb_processos_atualizacao


## Funções auxiliares:
# "src/generalFunctions/agora.R"
# "src/findosc-v2023.R"

## Outputs:
# Tb_OSC_Full (objeto da memória e, caso definicoes$att_teste == FALSE, arquivo salvo)

# bibliotecas necessárias:
library(magrittr)
library(dplyr)
library(dbplyr)
library(glue)
library(stringr)
library(lubridate)
library(assertthat)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identificação OSC ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito
# "21": Processo 2 (Identificação OSC via Razão Social) e 1 (completo)
if(!(21 %in% processos_att_atual)) {
  
  message("Identificação OSC via Razão Social")
  
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 21], 20))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 2, 
    processo_nome = "Identificação OSC")
  
  # Se os dados da Receita Federal não estiverem carregados, carrega eles. ####
  if(!(exists("tb_JoinOSC") && class(tb_JoinOSC) == "data.frame")) {
    
    path_file_backup <- glue("{diretorio_att}intermediate_files/tb_JoinOSC.RDS")
    
    # Garante que o arquivo existe.
    assert_that(file.exists(path_file_backup), 
                msg = paste0("Arquivo '", path_file_backup, "' não encontrado."))
    
    # Carrega o arquivo com os dados da busca SQL base SRF
    message(agora(), "   Carregando dados previamente baixados da RFB...")
    tb_JoinOSC <- readRDS(path_file_backup)
    rm(path_file_backup)
  }
  
  # Filtra por CNAE ####
  # 8550301	- Administração de caixas escolares
  # 94201 - Atividades de organizações sindicais
  # 8112500 - Condomínios prediais
  # 9492800 (em conjunto com certos nomes de razão social) - Atividades de organizações políticas
  # Fonte: https://concla.ibge.gov.br/busca-online-cnae.html?view=estrutura
  
  message(agora(), "   Iniciando verificação de partidos políticos")
  
  # Expressões regulares que indicam grupos partidários:
  indicadores_partidos <- paste(c("^COLIGACAO", "^COLIGAO", "^COMISSAO EXECUTIVA", 
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
  
  
  # Coloca aqui o nome dos campos para não ficar uma linha de comando muito
  # longa abaixo:
  
  # Carrega os campos necessários para os testes abaixo.
  CamposAtualizacao <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita)
  
  campo_cnae <- CamposAtualizacao %>% 
    dplyr::filter(campos == "campo_rfb_cnae_principal") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
    
  campo_razao_social <-  CamposAtualizacao %>% 
    dplyr::filter(campos == "campo_rfb_razao_social") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  # Evita nomes duplicados
  if(anyDuplicated(names(tb_JoinOSC)) > 0) {
    names(tb_JoinOSC) <- make.unique(names(tb_JoinOSC), sep = "_")
  }
  
  # Descobre OSC com base no CNAE e expressões regulares partidárias
  tb_JoinOSC <- tb_JoinOSC %>% 
    mutate(IsOSC = 
             case_when(
               .data[[campo_cnae]] %in% c("8112500", "8550301") ~ FALSE,
               str_sub(.data[[campo_cnae]], 1, 5) == "94201" ~ FALSE,
               str_detect(.data[[campo_razao_social]], indicadores_partidos) &
                 .data[[campo_cnae]] == "9492800" ~ FALSE,
               TRUE ~ TRUE)
           )
  
  # table(tb_JoinOSC$IsOSC, useNA = "always")
  rm(indicadores_partidos, campo_cnae, campo_razao_social)
  
  # Usa função find_OSC para determinar se um estabelecimento é OSC:
  
  # Expressões usadas em findosc
  NonOSCNames <- fread("tab_auxiliares/NonOSCNames.csv") %>% 
    as_tibble()
  # names(NonOSCNames)
  
  # Uso da função find_OSC para excluir organizações do conceito de OSC ####
  # com base em expressões regulares aplicadas à razão social:
  source("src/specificFunctions/findosc-v2023.R")
  
  message(agora(), "   Início da execução de find_OSC")
  
  # FindOSC:
  tb_JoinOSC$IsOSC <- tb_JoinOSC$IsOSC & 
    find_OSC(tb_JoinOSC$razao_social, NonOSCNames, verbose = FALSE)
  
  message(agora(), "   Fim da execução de find_OSC")
  
  rm(NonOSCNames, find_OSC)
  
  #  Inserindo OSCs que estavam na última versão do Banco ####
  # (princípio de não deletar OSC do banco exclusivamente pelo find_OSC)
  # bkp <- tb_JoinOSC
  # tb_JoinOSC <- bkp
  
  message("Incorporando legado do find_osc")
  
  # Tabela das osc
  tb_osc <- tbl(conexao_mosc, "tb_osc")
  
  # Resgatar as OSC da última atualização
  osc_ultima_att <- tb_osc %>% 
    dplyr::filter(bo_osc_ativa) %>% 
    select(cd_identificador_osc) %>% 
    collect() %>% 
    # Corrigir cd_identificador_osc (pad)
    mutate(cd_identificador_osc = str_pad(cd_identificador_osc, 
                                          width = 14, side = "left",
                                          pad = 0)) %>% 
    unlist() %>% as.character()
  
  # table(tb_JoinOSC$era_osc_ultima_att)
  
  # Colocar em tb_JoinOSC a variável de se ela estava ativa na última
  # atualização:
  tb_JoinOSC <- tb_JoinOSC %>% 
    mutate(era_osc_ultima_att = cnpj %in% osc_ultima_att,
    
           # Se houver alguma OSC identificada no passado, manter essa 
           # identificação:
           IsOSC = IsOSC | era_osc_ultima_att, 
           
           # Vou deixar NA para a fonte da atualização OSC quando tiver 
           # identificação da OSC pelo legado, para a informação do passado
           # prevaleça aqui:
           ft_IsOSC = ifelse(IsOSC, glue("findOSC.R_{codigo_presente_att}"), NA))
  
  rm(osc_ultima_att, tb_osc)
  
  # Extrai data de Fechamento das OSC ####
  tb_JoinOSC <- tb_JoinOSC %>% 
    # Campo de situação a atividade da OCS:
    mutate(cd_situacao_cadastral = as.integer(situacao_cadastral), 
           bo_osc_ativa = cd_situacao_cadastral %in% c(2, 3, 4), 
           # Existem casos em que a data_situacao_cadastral é '0', mesmo em OSC inativas.
           # Investiguei todos esses casos e me parece que são falhas de versões anteriores 
           # do find_osc, pois não me parecem OSC pelos critérios adotados. Vou excluir essas
           # OSC da amostra (são 1704 casos).
           data_situacao_cadastral = ifelse(data_situacao_cadastral == 0, 
                                            NA, data_situacao_cadastral),
           
           # A data de fechamento da OSC é a data da situação cadastral quando
           # ela está inativa:
           dt_fechamento_osc = ifelse(bo_osc_ativa, NA, 
                                      data_situacao_cadastral), 
           # Ano de fechamento OSC
           nr_ano_fechamento_osc = year(ymd(dt_fechamento_osc))
    )
  # table(tb_JoinOSC$bo_osc_ativa)
  # table(tb_JoinOSC$nr_ano_fechamento_osc, useNA = "always")
  
  # Salva tabela com o registro do fechamento OSC:
  DtFechamentoOSC <- tb_JoinOSC %>% 
    dplyr::filter(IsOSC, !is.na(dt_fechamento_osc)) %>% 
    rename(cd_identificador_osc = cnpj) %>%
    select(cd_identificador_osc, IsOSC, cd_situacao_cadastral, 
           dt_fechamento_osc, nr_ano_fechamento_osc) %>% 
    mutate(dt_ultima_att = as.character(today()))
  
  if(definicoes$salva_backup) saveRDS(DtFechamentoOSC, 
          glue("{diretorio_att}intermediate_files/DtFechamentoOSC.RDS"))
  
  # Muda nome do objeto para marcar mudança de processamento:
  Tb_OSC_Full <- tb_JoinOSC %>% 
    # Mantem apenas OSCs ativas no banco:
    mutate(situacao = as.integer(situacao_cadastral), 
           bo_osc_ativa = situacao %in% c(2, 3, 4)) %>%
    dplyr::filter(IsOSC, bo_osc_ativa)
  
  # Salva arquivo Backup ####
  path_file_backup <- glue("{diretorio_att}intermediate_files/Tb_OSC_Full.RDS")
  
  if(definicoes$salva_backup) saveRDS(Tb_OSC_Full, path_file_backup)
  
  # Atualiza controle de processos ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 20], 21))

  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 2, 
    path_file_backup = ifelse(definicoes$salva_backup, path_file_backup, NULL))
  
  # Salva arquivo de data de fechamento
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "arquivo backup", 
    id_att = id_presente_att, 
    id_processo = 2, 
    path_file_backup = glue("{diretorio_att}intermediate_files/DtFechamentoOSC.RDS"))
  
  rm(path_file_backup)
  rm(DtFechamentoOSC)
  rm(tb_JoinOSC) # não vamos mais utilizar esses dados
  
  } else {
    
    # Caso o processo já tenha sido feito anteriormente ####
    assert_that(exists("Tb_OSC_Full") || 
                  file.exists(
                    glue("{diretorio_att}intermediate_files/Tb_OSC_Full.RDS")),
                
                msg = glue("Não foi encontrado o objeto 'Tb_OSC_Full' na ", 
                           "memória ou em arquivos backup. Verificar porque o ", 
                           "processo 21 consta como concluído!")) %>% 
      if(.)  message("Identificação OSC via Razão Social já feita anteriormente")
    
  }
  

# Fim ####
  