# Instituto de Economia Aplicada - IPEA

# Objetivo do Script: formatar o output do GALILEO para poder gerar as
# variáveis de geolocalização do MOSC

# Autor do Script: Murilo Junqueira 
# (m.junqueira@yahoo.com.br; murilo.junqueira@ipea.gov.br)

# Data de Criação do Scrip: 2026-01-20


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Inputs:
# definicoes

## Funções auxiliares:

## Outputs:


# bibliotecas necessárias:


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Extrai endereços das OSC (somente OSC novas ou que mudaram) ####
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Executa o processo se não foi feito
if( !(121 %in% processos_att_atual) ) {
  
  message("Insere dados do TransfereGOV")
  Sys.sleep(2) # Dar um tempo apenas para o usuário ler as mensagens da atualização
  
  ## Início do processo ####
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 121], 120))
  
  # Atualiza controle de processos (tb_processos_atualizacao)  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "inicio", 
    id_att = id_presente_att, 
    id_processo = 12, 
    processo_nome = "Insere dados do TransfereGOV")
  
  
  # URLs e nomes dos arquivos
  CamposAtualizacao <- fread("tab_auxiliares/CamposAtualizacao.csv") %>% 
    dplyr::filter(schema_receita == definicoes$schema_receita)
  
  url_proposta <- CamposAtualizacao %>% 
    dplyr::filter(campos == "url_proposta") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  url_convenio <- CamposAtualizacao %>% 
    dplyr::filter(campos == "url_convenio") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character()
  
  zip_proposta <- CamposAtualizacao %>% 
    dplyr::filter(campos == "zip_proposta") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character() %>% 
    paste0(diretorio_att, "input_files/", .)
  
  zip_convenio <- CamposAtualizacao %>% 
    dplyr::filter(campos == "zip_convenio") %>% 
    select(nomes) %>% slice(1) %>%  unlist() %>% as.character() %>% 
    paste0(diretorio_att, "input_files/", .)
  
  if(!file.exists(zip_proposta) | !file.exists(zip_convenio)) {
    # Download dos arquivos
    message("Baixando arquivos...")
    
    # Cria diretório siconv
    if(!dir.exists(glue(diretorio_att, "/input_files/dados_siconv")) ) { 
      dir.create(glue(diretorio_att, "/input_files/dados_siconv"))
    }
    
    download.file(url_proposta, destfile = zip_proposta, mode = "wb")
    download.file(url_convenio, destfile = zip_convenio, mode = "wb")
    
    ## Descompactar dentro da pasta central
    print("Descompactando arquivos...")
    unzip(zip_proposta, exdir = paste0(diretorio_att, "input_files/"))
    unzip(zip_convenio, exdir = paste0(diretorio_att, "input_files/"))
  }
  
  # Identificar os arquivos CSV
  csvs <- list.files(paste0(diretorio_att, "input_files/"), 
                     pattern = "^siconv", 
                     full.names = TRUE) %>% 
    str_subset("csv$")
  
  csv_proposta <- str_subset(csvs, "proposta")
  csv_convenio <- str_subset(csvs, "convenio")
  rm(csvs, zip_proposta, zip_convenio, url_proposta, url_convenio, 
     CamposAtualizacao)
  
  
  # Ler os dados com codificação Latin1 (UTF-8) e converter para data.table
  print("Lendo dados dos arquivos CSV...")
  
  
  dados_proposta <- fread(csv_proposta, 
                          encoding = "UTF-8", 
                          sep = ";", 
                          # nrows = 10000,
                          select = c("ID_PROPOSTA", 
                                     "COD_MUNIC_IBGE", 
                                     "IDENTIF_PROPONENTE",
                                     "OBJETO_PROPOSTA"),
                          dec = ",")
  
  dados_convenio <- fread(csv_convenio, 
                          encoding = "UTF-8", 
                          sep = ";", 
                          # nrows = 10000,
                          select = c("ID_PROPOSTA",
                                     "NR_CONVENIO",
                                     "SIT_CONVENIO",        
                                     "DIA_INIC_VIGENC_CONV",
                                     "DIA_FIM_VIGENC_CONV",
                                     "VL_GLOBAL_CONV",
                                     "VL_REPASSE_CONV"),
                          dec = ",")
  
  # Mesclar os dados usando data.table
  print("Processando dados...")
  
  dados_mesclados <- dados_convenio %>% 
    left_join(dados_proposta, by = "ID_PROPOSTA") 
  
  # Limpa memória
  rm(dados_convenio, dados_proposta)
  gc()
  
  
  # Resgata o ID já existente das OSC
  idControl <- tbl(conexao_mosc, "tb_osc") %>% 
    select(id_osc, cd_identificador_osc) %>% 
    collect() %>% 
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0"))
  
  # Criar a variável fonte_carga
  fonte_carga <-  paste0("SICONV/MPOG ", today())
  
  
  # Organiza os dados
  transferegov_tidy <- dados_mesclados %>% 
    
    # Algumas variáveis, basta alterar o nome:
    rename(
      cd_identificador_osc = IDENTIF_PROPONENTE,
      nr_valor_captado_projeto = VL_REPASSE_CONV,
      nr_valor_total_projeto = VL_GLOBAL_CONV,
      cd_municipio = COD_MUNIC_IBGE
      ) %>% 
    
    # Resgata o id_osc antigo.
    mutate(cd_identificador_osc = str_pad(as.character(cd_identificador_osc), 
                                          width = 14, 
                                          side = "left", 
                                          pad = "0")) %>% 
    left_join(idControl, by = "cd_identificador_osc") %>% 
    
    # Exclui as organizações que não são OSC
    dplyr::filter(!is.na(id_osc)) %>% 
    
    # Formata dados:
    mutate(
      
      id_projeto = NA_integer_,
      tx_nome_projeto = paste0("Parceria ", NR_CONVENIO),
      
      cd_status_projeto = case_when(
        
        SIT_CONVENIO %in% c(
          "Proposta/Plano de Trabalho Aprovado",
          "Proposta/Plano de Trabalho Complementado Enviado para Análise"
        ) ~ 3, # Proposta
        
        SIT_CONVENIO %in% c(
          "Em execução", "Assinado", 
          "Assinado Pendente Registro TV Siafi", 
          "Assinatura Pendente Registro TV Siafi"
        ) ~ 4, # Projeto em Andamento
        
        SIT_CONVENIO %in% c(
          "Inadimplente", "Aguardando Prestação de Contas", 
          "Prestação de Contas Aprovada",
          "Prestação de Contas Aprovada com Ressalvas", 
          "Prestação de Contas em Análise",
          "Prestação de Contas em Complementação", 
          "Prestação de Contas enviada para Análise",
          "Prestação de Contas Iniciada Por Antecipação", 
          "Prestação de Contas Rejeitada",
          "Prestação de Contas Concluída",
          "Prestação de Contas Comprovada em Análise"
        ) ~ 2, # Finalizado
        
        SIT_CONVENIO %in% c("Cancelado", "Convênio Anulado"
                            ) ~ 1, # Arquivado,  cancelado ou indeferido
        
        TRUE ~ 5, # Outro
        ),
      
      # Converte datas
      dt_data_inicio_projeto = dmy(DIA_INIC_VIGENC_CONV),
      dt_data_fim_projeto = dmy(DIA_FIM_VIGENC_CONV),
      
      # Variáveis não disponíveis
      # (precisam ser inseridas pelo usuário)
      tx_link_projeto = NA_character_,
      nr_total_beneficiarios = NA_integer_,
      cd_abrangencia_projeto = NA_integer_,
      cd_zona_atuacao_projeto = NA_integer_,
      tx_metodologia_monitoramento = NA_character_,
      tx_status_projeto_outro = NA_character_, # TODO: o que é essa variável?
      
      # Substituir aspas simples ' por aspas duplas " na coluna OBJETO_PROPOSTA
      OBJETO_PROPOSTA = str_replace_all(OBJETO_PROPOSTA, "'", '"'),
      tx_descricao_projeto = OBJETO_PROPOSTA,
      
      # Coloca o número do convênio e da proposta do id exeterno
      tx_identificador_projeto_externo = paste0("proposta:", ID_PROPOSTA,
                                                "|", 
                                                "convenio:", NR_CONVENIO),
      bo_oficial = TRUE,
      cd_uf = as.integer(str_sub(as.character(cd_municipio), 1, 2)),
      
      # Campos ft
      ft_status_projeto = fonte_carga,
      ft_nome_projeto = fonte_carga,
      ft_data_inicio_projeto = fonte_carga,
      ft_data_fim_projeto = fonte_carga,
      ft_link_projeto = NA_character_,
      ft_total_beneficiarios = NA_character_,
      ft_valor_captado_projeto = fonte_carga,
      ft_valor_total_projeto = fonte_carga,
      ft_abrangencia_projeto = NA_character_,
      ft_zona_atuacao_projeto = NA_character_,
      ft_descricao_projeto = fonte_carga,
      ft_metodologia_monitoramento = NA_character_,
      ft_identificador_projeto_externo = fonte_carga,
      ft_municipio = fonte_carga,
      ft_uf = fonte_carga,
      NR_CONVENIO = as.character(NR_CONVENIO),
      ) %>% 
    
    # Organiza dados por início do projeto:
    arrange(dt_data_inicio_projeto) %>% 
    
    # Seleciona colunas e arruma a ordem
    select(
      id_projeto,
      id_osc,
      tx_nome_projeto,
      ft_nome_projeto,
      cd_status_projeto,
      # SIT_CONVENIO,
      ft_status_projeto,
      dt_data_inicio_projeto,
      ft_data_inicio_projeto,
      dt_data_fim_projeto,
      ft_data_fim_projeto,
      tx_link_projeto,
      ft_link_projeto,
      nr_total_beneficiarios,
      ft_total_beneficiarios,
      nr_valor_captado_projeto,
      ft_valor_captado_projeto,
      nr_valor_total_projeto,
      ft_valor_total_projeto,
      cd_abrangencia_projeto,
      ft_abrangencia_projeto,
      cd_zona_atuacao_projeto,
      ft_zona_atuacao_projeto,
      tx_descricao_projeto,
      ft_descricao_projeto,
      ft_metodologia_monitoramento,
      tx_metodologia_monitoramento,
      tx_identificador_projeto_externo,
      ft_identificador_projeto_externo,
      bo_oficial,
      tx_status_projeto_outro,
      cd_municipio,
      ft_municipio,
      cd_uf,
      ft_uf,
      NR_CONVENIO,
    )
  
  rm(dados_mesclados, idControl, fonte_carga)
  
  # Cria nova chave primária para os dados:
  
  ## Baixa chaves antigas:
  
  # Resgata o ID já existente das OSC
  idProjetos <- tbl(conexao_mosc, "tb_projeto") %>% 
    select(id_projeto, tx_identificador_projeto_externo) %>% 
    collect() %>% 
    rename(old_id = id_projeto) %>% 
    # Formata dados
    mutate(
      NR_CONVENIO = tx_identificador_projeto_externo, 
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed(".")),
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed("-")),
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed(" ")),
      NR_CONVENIO = ifelse(NR_CONVENIO == "", NA, NR_CONVENIO),
      NR_CONVENIO = str_remove(NR_CONVENIO, "\\|convenio.*"),
      NR_CONVENIO = str_remove(NR_CONVENIO, fixed("proposta:")),
      
    ) %>% 
    select(-tx_identificador_projeto_externo)
  
  
  # Insere id nos convênios existentes:
  tb_projeto <- transferegov_tidy %>% 
    left_join(idProjetos, by = "NR_CONVENIO") %>% 
    mutate(id_projeto = old_id) %>% 
    select(-old_id)

  # sum(is.na(tb_projeto$id_projeto))
  # nrow(tb_projeto) - sum(is.na(tb_projeto$id_projeto))

  # Insere novos IDs:
  NovosIds <- max(idProjetos$old_id) + seq_len(sum(is.na(tb_projeto$id_projeto)))
  tb_projeto$id_projeto[is.na(tb_projeto$id_projeto)] <- NovosIds
  
  rm(NovosIds, idProjetos, transferegov_tidy)
  
  # Salva arquivos de endereços das OSC
  saveRDS(tb_projeto, glue("{diretorio_att}output_files/tb_projeto.RDS"))
  
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 120], 121))
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 121, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              glue("{diretorio_att}output_files/tb_projeto.RDS"), 
                              NULL))
  
  
  # Deleta arquivos descompatactados (deixa os compactados)
  unlink(csv_convenio)
  unlink(csv_proposta)
  rm(csv_convenio, csv_proposta)
  rm(tb_projeto)
  gc()
  # ls()
  
} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("tb_projeto") || 
                file.exists(
                  glue("{diretorio_att}intermediate_files/tb_projeto.RDS")),
              
              msg = glue("Não foi encontrado o objeto 'tb_projeto' na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 12 consta como concluído!")) %>% 
    if(.)  message("Extração dos dados do TransfedeGov realizada ", 
                   "anteriormente!")
  
}


# Fim ####