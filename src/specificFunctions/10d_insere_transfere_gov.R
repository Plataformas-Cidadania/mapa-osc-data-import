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
library(httr)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Extrai informações do TransfereGov ####
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
    pull(nomes)
  
  url_convenio <- CamposAtualizacao %>% 
    dplyr::filter(campos == "url_convenio") %>% 
    pull(nomes)
  
  url_emendas <- CamposAtualizacao %>% 
    dplyr::filter(campos == "url_emendas") %>% 
    pull(nomes)
  
  zip_proposta <- CamposAtualizacao %>% 
    dplyr::filter(campos == "zip_proposta") %>% 
    pull(nomes) %>% 
    paste0(diretorio_att, "input_files/", .)
  
  zip_convenio <- CamposAtualizacao %>% 
    dplyr::filter(campos == "zip_convenio") %>% 
    pull(nomes) %>% 
    paste0(diretorio_att, "input_files/", .)
  
  zip_emendas <- CamposAtualizacao %>% 
    dplyr::filter(campos == "zip_emendas") %>% 
    pull(nomes) %>% 
    paste0(diretorio_att, "input_files/", .)
  
  # Garante que todas regerências são únicas:
  assert_that(
    c(url_convenio, url_emendas, url_proposta, zip_convenio, zip_emendas,
      zip_proposta) %>% 
      map_int(length) %>% 
      magrittr::equals(1) %>% 
      all()
  )
  
  
  if(!file.exists(zip_proposta) | !file.exists(zip_convenio)) {
    # Download dos arquivos
    message("Baixando arquivos...")
    
    # Cria diretório siconv
    if(!dir.exists(glue(diretorio_att, "/input_files/dados_siconv")) ) { 
      dir.create(glue(diretorio_att, "/input_files/dados_siconv"))
    }
    
    library(httr)
    
    # GET ignora a verificação de SSL e grava direto no disco
    GET(
      url = url_proposta,
      write_disk(zip_proposta, overwrite = TRUE),
      config(ssl_verifypeer = FALSE) 
    )
    
    GET(
      url = url_convenio,
      write_disk(zip_convenio, overwrite = TRUE),
      config(ssl_verifypeer = FALSE) 
    )
    
    GET(
      url = url_emendas,
      write_disk(zip_emendas, overwrite = TRUE),
      config(ssl_verifypeer = FALSE) 
    )
  }
  
  ## Descompactar dentro da pasta central
  print("Descompactando arquivos...")
  unzip(zip_proposta, exdir = paste0(diretorio_att, "input_files/"))
  unzip(zip_convenio, exdir = paste0(diretorio_att, "input_files/"))
  unzip(zip_emendas, exdir = paste0(diretorio_att, "input_files/"))
  
  # Identificar os arquivos CSV
  csvs <- list.files(paste0(diretorio_att, "input_files/"), 
                     pattern = "^siconv", 
                     full.names = TRUE) %>% 
    str_subset("csv$")
  
  csv_proposta <- str_subset(csvs, "proposta")
  csv_convenio <- str_subset(csvs, "convenio")
  csv_emenda <- str_subset(csvs, "emenda")
  
  rm(csvs, zip_proposta, zip_convenio, url_proposta, url_convenio, 
     zip_emendas, url_emendas, CamposAtualizacao)
  
  
  # Ler os dados com codificação Latin1 (UTF-8) e converter para data.table
  print("Lendo dados dos arquivos CSV...")
  
  # Corrige problema de caracteres incorretos dos nomes
  proposta_names <- fread(csv_proposta, 
                          nrows = 10,
                          encoding = "UTF-8", 
                          sep = ";", 
                          dec = ",")
  
  # names(proposta_names)
  
  # Limpa o BOM da primeira coluna modificando por referência 
  # (zero custo de memória)
  setnames(
    x = proposta_names, 
    old = names(proposta_names)[1], 
    new = gsub("[^[:alnum:]_]", "", names(proposta_names)[1])
  )
  
  # Para selecionar apenas as colunas relevantes
  sel_col <- names(proposta_names) %in% 
    c("ID_PROPOSTA",
      "COD_MUNIC_IBGE",
      "NR_PROPOSTA",
      "IDENTIF_PROPONENTE",
      "OBJETO_PROPOSTA") %>% 
    which()
  
  dados_proposta <- fread(
    csv_proposta, 
    # nrows = 10000, # debug
    encoding = "UTF-8", 
    dec = ",",
    sep = ";", 
    select = sel_col
  ) %>% 
    magrittr::set_names(
      c("ID_PROPOSTA",
        "COD_MUNIC_IBGE",
        "NR_PROPOSTA",
        "IDENTIF_PROPONENTE",
        "OBJETO_PROPOSTA")
    )
  
  rm(proposta_names, sel_col)
  
  dados_convenio <- fread(
    csv_convenio, 
    encoding = "UTF-8", 
    sep = ";", 
    # nrows = 10000,  # debug
    select = c("ID_PROPOSTA",
               "NR_CONVENIO",
               "SIT_CONVENIO",
               "DIA_INIC_VIGENC_CONV",
               "DIA_FIM_VIGENC_CONV",
               "VL_GLOBAL_CONV",
               "VL_REPASSE_CONV"),
    dec = ",")
  
  # Dados das emendas parlamentares:
  dados_emendas <- fread(csv_emenda, select = "ID_PROPOSTA") %>% 
    # Deixa apenas emendas relacionadas a convênios:
    dplyr::filter(ID_PROPOSTA != "", 
                  !is.na(ID_PROPOSTA)) %>% 
    pull(ID_PROPOSTA)
  
  # Mesclar os dados 
  print("Processando dados...")
  
  dados_mesclados <- dados_convenio %>% 
    left_join(dados_proposta, by = "ID_PROPOSTA") %>% 
    mutate(
      bo_possui_emenda = ID_PROPOSTA %in% dados_emendas
    )
  
  # análise dos dados:
  if(FALSE) {
    sum(dados_mesclados$bo_possui_emenda)
    
    names(dados_mesclados)
    
    dados_mesclados %>% 
      mutate(Ano = year(dmy(DIA_FIM_VIGENC_CONV))) %>% 
      group_by(Ano) %>% 
      summarise(Per = mean(bo_possui_emenda, rm.na = TRUE)) %>% 
      print(n = 30)
  }
  
  
  # Limpa memória
  rm(dados_convenio, dados_proposta, dados_emendas)
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
      cd_municipio = COD_MUNIC_IBGE,
      nr_numero_proposta = NR_PROPOSTA,
      nr_numero_instrumento = NR_CONVENIO,
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
      tx_nome_projeto = paste0("Parceria ", nr_numero_instrumento),
      
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
                                                "convenio:", nr_numero_instrumento),
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
      ft_numero_proposta = fonte_carga,
      ft_numero_instrumento = fonte_carga,
      ft_possui_emenda = fonte_carga,
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
      bo_possui_emenda,
      ft_possui_emenda,
      nr_numero_proposta,
      ft_numero_proposta,
      nr_numero_instrumento,
      ft_numero_instrumento,
    )
  
  rm(dados_mesclados, idControl)
  
  # Cria nova chave primária para os dados:
  
  ## Baixa chaves antigas:
  
  # Resgata o ID já existente das OSC
  idProjetos <- tbl(conexao_mosc, "tb_projeto") %>% 
    select(id_projeto, cd_status_projeto, tx_identificador_projeto_externo, 
           nr_numero_instrumento) %>% 
    collect() %>% 
    rename(old_id = id_projeto) %>% 
    # Formata dados
    mutate(
      NR_CONVENIO = tx_identificador_projeto_externo, 
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed(".")),
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed("-")),
      NR_CONVENIO = str_remove_all(NR_CONVENIO, fixed(" ")),
      NR_CONVENIO = ifelse(NR_CONVENIO == "", NA, NR_CONVENIO),
      NR_CONVENIO = str_extract(NR_CONVENIO, "convenio.*"),
      NR_CONVENIO = str_remove(NR_CONVENIO, fixed("convenio:")),
      NR_CONVENIO = as.integer(NR_CONVENIO),
      
      # Dar preferência à variável 'nr_numero_instrumento', caso ela não 
      # estiver presente, usar NR_CONVENIO:
      nr_numero_instrumento = ifelse(is.na(nr_numero_instrumento), NR_CONVENIO, 
                                     nr_numero_instrumento), 
      
      nr_numero_instrumento = as.integer(nr_numero_instrumento),
      
      # Verifica se há casos de projetos do MOSC que "desapareceram" 
      # do TransfereGOV:
      check_missing = 
        !(nr_numero_instrumento %in% transferegov_tidy$nr_numero_instrumento),
      
      # Só vale para projetos finalizados ou em andamento:
      check_missing = check_missing & (cd_status_projeto %in% c(2, 4, 5) ),
    ) 
  
  # sum(idProjetos$check_missing)
  
  # Base para se alterar o status de projetos excluídos da base
  alterar_status <- idProjetos %>% 
    dplyr::filter(check_missing) %>% 
    mutate(id_projeto = old_id, 
           cd_status_projeto = 6L, 
           ft_status_projeto = fonte_carga) %>% 
    select(id_projeto, cd_status_projeto, ft_status_projeto)
  
  # Insere id nos convênios existentes:
  tb_projeto <- transferegov_tidy %>% 
    left_join(select(idProjetos, old_id, nr_numero_instrumento), 
              by = "nr_numero_instrumento") %>% 
    mutate(id_projeto = old_id) %>% 
    select(-old_id) %>% 
    bind_rows(alterar_status)
  
  # sum(is.na(tb_projeto$id_projeto))
  # nrow(tb_projeto) - sum(is.na(tb_projeto$id_projeto))
  
  # Insere novos IDs:
  NovosIds <- max(idProjetos$old_id) + seq_len(sum(is.na(tb_projeto$id_projeto)))
  tb_projeto$id_projeto[is.na(tb_projeto$id_projeto)] <- NovosIds
  
  rm(NovosIds, idProjetos, transferegov_tidy, alterar_status, fonte_carga)
  
  # Salva arquivos de endereços das OSC
  saveRDS(tb_projeto, glue("{diretorio_att}output_files/tb_projeto.RDS"))
  
  # Deleta arquivos descompatactados (deixa os compactados)
  unlink(csv_convenio)
  unlink(csv_proposta)
  unlink(csv_emenda)
  
  rm(csv_convenio, csv_proposta, csv_emenda)
  rm(tb_projeto)
  gc()
  # ls()
  
  processos_att_atual <- unique(c(processos_att_atual[processos_att_atual != 120], 121))
  
  if(!definicoes$att_teste) atualiza_processos_att(
    TipoAtt = "fim", 
    id_att = id_presente_att, 
    id_processo = 12, 
    path_file_backup = ifelse(definicoes$salva_backup, 
                              glue("{diretorio_att}output_files/tb_projeto.RDS"), 
                              NULL))

} else {
  
  # Caso o processo já tenha sido feito anteriormente ####
  assert_that(exists("tb_projeto") || 
                file.exists(
                  glue("{diretorio_att}output_files/tb_projeto.RDS")),
              
              msg = glue("Não foi encontrado o objeto 'tb_projeto' na ", 
                         "memória ou em arquivos backup. Verificar porque o ", 
                         "processo 12 consta como concluído!")) %>% 
    if(.)  message("Extração dos dados do TransfedeGov realizada ", 
                   "anteriormente!")
  
}


# Fim ####