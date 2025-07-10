

# Data da Criação 2025-06-16

# Download do ZIP do TransfereGOV
## Propostas e Convênios

# Descompactar o ZIP e pegar o arquivo CSV


# Fazer o MERGE dos dois arquivos: proposta e convênios
## PK: ID_PROPOSTA
## INNER JOIN
## Não diferença quem gruda no que

# Carrega o arquivo mesclado

# Cria variável de 'cd_status projeto'
## usa a variável SIT_CONVENIO
## Transforma a string em integer

# Cria a variável 'link'
## O link da página, mas id do convênio

# Cria a variável 'FONTE_CARGA', representando a corigem do DADO

# Pega apenas as colunas utilizadas no MOSC

# Atuliza a tabela 'mapa_osc.tb_projeto' do psql-10

# Migra do psql-10 para o psql-12


# Investigar o schema log



