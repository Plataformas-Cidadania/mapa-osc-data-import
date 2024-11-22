Mapa das Organizações da Sociedade Civil
================
Cooordenação de Instituições Políticas, Governça e Relações
Interfederativas da Diretoria de Estudos e Políticas sobre o Estado, as
Instituições e a Democracia (Coins/Diest) - IPEA, Governo Federal,
Brasil

## Rotina de Atualização - versão 2023/2024

A atualização do banco de dados Mapa das Organizações Sociais (MOSC)
refere-se à incorporação de novos dados provenientes de fontes mais
recentes, como a base de dados da Receita Federal, RAIS ou outras
fontes. É importante não confundir a “atualização do banco de dados
MOSC” com uma “atualização geral do sistema MOSC”, que pode envolver
também mudanças no site ou na estrutura do banco de dados. Estamos
falando aqui estritamente da inserção de dados novos ou atualização de
dados existentes, mantendo inalterada outras partes do MOSC. Desde a
versão 2023/2024 do MOSC, todo o processo de atualização de dados foi
automatizado usando a linguagem R, conforme ilustrado no fluxograma da
figura 1:

### Figura 1

<figure>
<img src="documentacao/Fluxograma%20MOSC%20-%20Fluxograma%20MOSC.png"
alt="Fluxograma de Atualização MOSC" />
<figcaption aria-hidden="true">Fluxograma de Atualização
MOSC</figcaption>
</figure>

A atualização inicia-se com a inserção de novos dados nos bancos de
dados disponíveis ao IPEA, principalmente da Receita Federal. Dados da
RAIS e outras fontes (descritas adiante) também são utilizados. Esses
dados podem estar nos bancos de dados instalados nos servidores do IPEA
(PostgreSQL) ou em arquivos (formato CSV ou RDS) em uma pasta
específica. Em seguida, a rotina de atualização em R (detalhada
posteriormente) processa os dados, utilizando várias tabelas auxiliares.
Essas tabelas contêm informações sobre códigos de variáveis,
correspondências entre diferentes códigos e expressões regulares usadas
para classificar os dados.

A etapa de geolocalização ainda não está incorporada ao script R e
requer um software separado. Atualmente, o programa utilizado é o
GALILEO. Assim, o processamento dos dados em R é executado até a etapa
de geoprocessamento, quando é interrompido para que o programa de
geolocalização produza a latitude e longitude das OSCs. Depois, o script
é reiniciado para concluir o processo.

O script R é programado para salvar os arquivos intermediários e finais
da atualização em uma pasta de backup. Isso permite a recuperação dos
dados e garante a completa rastreabilidade e reprodutibilidade do
processo. Por fim, o script R atualiza os dados do banco de dados MOSC
nos servidores do IPEA.

Após a atualização dos dados, o site MOSC é atualizado por meio da
programação backend e frontend, gerando novas tabelas, figuras e páginas
de Organizações da Sociedade Civil. No entanto, esta documentação aborda
apenas as etapas de atualização dos dados propriamente dita, não
tratando de questões de frontend ou backend.

## Como Instalar a Rotina de Atualização

A rotina de atualização pode ser instalada em qualquer computador
que: 1. Tenha permissão para ler as fontes de dados (Receita Federal e
RAIS, principalmente), 2. tenha permissão para manipular os bancos de
dados do Mapa das Organizações da Sociedade Civil e 3. possua os
softwares necessários instalados (R, RStudio e pacotes do R). A seguir,
descrevemos detalhadamente o processo de instalação.

**Passo 01:** Instalar o R e o RStudio. O computador que executará a
rotina de atualização precisa ter o R e o RStudio instalados. O processo
de instalação é semelhante ao de qualquer outro software. No entanto, é
importante atentar-se às diferenças de instalação entre os diversos
sistemas operacionais (Linux, Windows, macOS etc.). O R é uma linguagem
de programação livre voltada para estatística, análise e manipulação de
dados. Seus recursos são flexíveis e poderosos o suficiente para
executar toda a rotina de atualização do MOSC (com exceção da
geolocalização). O RStudio é um IDE (Integrated Development
Environment), criado pela empresa Posit Software, PBC, para facilitar e
automatizar a programação em linguagem R. Embora seja possível usar o R
sem o RStudio, essa prática é incomum, dada a grande popularidade dessa
combinação. Além disso, toda a programação da atualização do MOSC foi
feita como um R Project (Projeto R) do RStudio, de modo que não há
garantia de que funcionará fora dele. É importante ressaltar que, até a
data de elaboração desta documentação (julho de 2024), não é necessário
adquirir nenhuma versão paga do RStudio, pois a versão gratuita para uso
individual é suficiente para executar todo o código.

**Passo 02:** instalar os pacotes necessários do R. O R permite que o
usuário instale pacotes (conjuntos de funções) criados por usuários
externos ao R core team. A atualização do MOSC utiliza alguns desses
pacotes, que precisam ser instalados previamente. A lista completa dos
pacotes necessários para a atualização encontra-se abaixo. Para
instalação em Linux, é crucial verificar se todos os pacotes estão
funcionando corretamente, garantindo que todas as bibliotecas
necessárias do sistema operacional estejam presentes. Pacotes de
programação e manipulação de dados: magrittr, tidyverse, assertthat.
Pacotes de leitura de dados externos: data.table, readxl, jsonlite.
Pacote de manipulação de datas: lubridate. Pacotes de manipulação de
textos: stringr, glue. Pacotes de conexão a bancos de dados: DBI, RODBC,
RPostgres, dbplyr.

**Passo 03:** baixar a rotina de atualização no diretório GitHub. As
rotinas de atualização e as tabelas auxiliares estão disponíveis no
repositório GitHub:
<https://github.com/Plataformas-Cidadania/mapa-osc-data-import> . O
usuário pode clonar este repositório ou baixar os códigos diretamente no
site. É fundamental assegurar que o diretório de trabalho do R seja o
local onde o repositório Git está instalado.

**Passo 04:** conseguir as credenciais de acesso aos bancos de dados
‘psql12/portal_osc’ e ‘psql10-df/rais_2019’. A rotina de atualização lê
os dados da versão mais recente da RAIS e da Receita Federal no banco de
dados ‘psql10-df/rais_2019’ do IPEA e atualiza os dados do banco de
dados ‘psql12/portal_osc’. Para acessar esses bancos, são necessárias
chaves de acesso específicas. O ‘psql10-df/rais_2019’ requer apenas uma
credencial simples de leitura, enquanto o ‘psql12/portal_osc’ exige uma
credencial qualificada que permita ler dados de todas as tabelas,
modificar dados, criar novos campos, criar e excluir tabelas. As
credenciais devem ser armazenadas na pasta “keys” do diretório de
atualização, em arquivos JSON separados para ‘psql10-df/rais_2019’ e
‘psql12/portal_osc’. Cada arquivo deve conter cinco campos: username,
password, dbname, host e port. O modelo do arquivo JSON está disponível
abaixo, e o usuário deve substituir o conteúdo entre “\<\>” pelas
informações corretas.

{“username”:“<usuario>”, “password”:“<senha>”,
“dbname”:“<nome banco de dados>”, “host”:“<servidor>”,
“port”:“<porta de acesso>”}

Realizados todos os procedimentos acima, basta executar as linhas de
“src/OSC-v2023.R”, segundo descrição abaixo, para realizar a atualização
MOSC. Recomendamos executar “src/OSC-v2023.R” linha a linha, prestando
bastante atenção às instruções e contidas nos comentários ao código de
programação.

## Descrição sumária dos procedimentos de atualização

A etapa do processamento dos dados (caixa central da figura 1) é
executada pelo arquivo “src/OSC-v2023.R” que realiza 8 etapas:

1.  Extração os dados sobre as organizações na Receita Federal (base
    CNPJ);

2.  Usar uma série de rotinas específicas para identificar quais
    organizações são OSC, com a ajuda da função ‘find_OSC’;

3.  Identificação da área de atuação das OSC com base na CNAE (Cadastro
    Nacional de Atividades Econômicas), usando outra função específica
    (AreaAtuacaoOSC).

4.  Identificação da geolocalização (longitude e latitude) das
    organizações utilizando um software específico. Essa etapa não está
    atualmente incorporada ao código R e requer um software externo
    (como o GALILEO, por exemplo);

5.  Separação dos dados gerados nas etapas anteriores nas tabelas
    principais do MOSC: tb_osc, tb_dados_gerais, tb_localizacao,
    tb_contatos e tb_areas_atuacao. Na tabela tb_areas_atuacao, também
    podem ser usadas outras fontes de dados, como informações do
    Ministério da Saúde (CNES, CNEAS Saúde) e do Ministério do
    Desenvolvimento Social (Censo SUAS, CNEAS Assistência Social) e
    outras;

6.  Incorporação dados da RAIS, que mostram informações sobre as
    relações trabalhistas das OSC;

7.  Upload dos dados gerados nas etapas anteriores no banco de dados
    PostgreSQL do IPEA, alocando cada informação em seu devido lugar;

8.  Procedimentos finais de atualização, como atualizar as Views
    materializadas do banco de dados para que o site seja atualizado,
    registrar o término da atualização e salvar os procedimentos
    utilizados (inclusive o próprio script) para permitir uma completa
    rastreabilidade da atualização no futuro.

    Uma inovação introduzida na atualização de 2024 é a adição de três
    tabelas no Mapa para registrar o fluxo de atualização. São elas:

**tb_controle_atualizacao:** registra as atualizações do mapa, incluindo
a data e horário de início e fim da atualização, bem como a data de
referência (última atualização da Receita Federal) dos dados.

**tb_processos_atualizacao:** registra o horário de início e fim de cada
uma das oito etapas de atualização mencionadas anteriormente.

**tb_backups_files:** registra os arquivos intermediários e finais
gerados durante o processo de atualização.

A seguir, detalharemos cada um dos processos de atualização.

## Estrutura do Diretório

1.  src/DatabaseOutput.R  
2.  src/AnalysisDataOutput.R  
3.  src/Models.R

## Principais Arquivos de Atualização

## Tabelas Auxiliares

## Backup de Atualização
