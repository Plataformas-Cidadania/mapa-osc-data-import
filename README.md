Mapa das Organizações da Sociedade Civil - IPEA, Governo Federal, Brasil
================

# Descrição do Mapa

This is

# O Processo de Atualização dos Dados

There are three ways to replicate this research, depending on how deep
the replicantion is.

# Estrutura do Diretório

1.  src/DatabaseOutput.R  
2.  src/AnalysisDataOutput.R  
3.  src/Models.R

# Principais

The first one create the relational database available in “data/dataset”
that clean and connect all raw data. The second script create file
“data/analysisdata/DataAnalysis.csv”, from which the regression models,
figures and tables of the paper where generated. The last script yields
the regression models, figures and tables of the paper.

These three scripts use many functions available in the folders
“src/generalFunctions” and “src/specifcFuntions” (see [Scripts folder
section](#scriptsfolder)). The complete dependence network of the codes
and data files are avalible in “data/dataset/FileTracing.csv”

### 3.2 Software Dependencies

1.  R version 3.4.3 (2017-11-30) – “Kite-Eating Tree”
2.  Java
3.  Microsoft Access Database Engine (for windows, [download
    here](https://www.microsoft.com/en-us/download/details.aspx?id=54920))

The R packages needed to run the codes are described in the scripts.

## 4. Data <a name="datasection"></a>

Our workflow in this research is first transform the raw data in a
relational dataset that clean and connect all raw data (avalible in
“data/dataset”) and then use this relational dataset to create the
variables used in regression models, tables and figures (file
“data/analysisdata/DataAnalysis.csv). Almost all steps of data
collection, cleaning and filtering, as well as variable creation, are in
R codes. Basicaly, it is only necessary point and click to download data
from sources that don’t allow direct download or use captcha. There are
other few cases of point and click, basically to unzip or extract data
from accdb files. All this cases are indicated in R code comments.

We recommend a careful review of the files following files, that
describe the dataset and variable generation process:

- **data/dataset/Codebook.csv:** Description of all variables present in
  data and metadata files.

- **data/dataset/RawFilesList.csv:** List of raw data files. From this
  list came almost all the information used in this project, incluing
  data from our own research. The only data that are not in this list
  are data that came from IPEA and CEPESP APIs. This file contains the
  URL link to original data source.

- **data/dataset/FileTracing.csv:** Shows the relationship between the
  files of this project. Shows which files were used to generate other
  files, including programming codes, functions and data sources.

<a name="datawarning"></a>

Warning: Many databases files of this research are too large to be
uploaded to github, so we omit some data files from this repository.
Therefore, to a full replication, it is necessary go to original source,
download the data and then run the script “src/DatabaseOutput.R” to
rebuild the dataset. The file “data/dataset/RawFilesList.csv” shows the
URL links of the original source data and the correct folder to put the
files in order to make the R codes work.

### 4.1. Data sources <a name="datasources"></a>

Below are the summary of the data sources. For a more detailed view of
the data sources see “data/dataset/RawFilesList.csv”.

1.  **Spada, P. 2012**. “Brazilian Participatory Budgeting Census:
    1989-2012.” Available at:
    <http://participedia.net/en/content/brazilian-participatory-budgeting-census>
    / Access date: April 20, 2018. (licensed under CC BY-NC-SA 3.0).

2.  **IPEADATA:** A portal of the Brazilian Government Institute for
    Applied Economics Research, that puts together several data sources
    of the Brazilian Governement (<http://www.ipeadata.gov.br>). The
    data was downloaded by IPEA API through ipeadatar R package.

3.  **CEPESP:** Center for Politics and Economics of the Public Sector
    (CEPESP) of the Getulio Vargas Foundation (FGV) provides access to
    Brazilian electoral data through a user friendly platform called
    CepespData website (<http://cepespdata.io/>). We use its R package,
    called cepespR (<https://github.com/Cepesp-Fgv/cepesp-r>).

4.  **TSE:** Organized and documented access to the Brazilian Electoral
    Superior Court (TSE) data on elections in Brazil from 1998 to 2016.

5.  **FINBRA**: Offical Accounting Data for Brazilian Municipalities
    (1989-2016), gathered by National Treasury Secretary of the Ministry
    of Economy

6.  **Own research**: In many cases we had to create data, mainly to be
    able to connect data from different sources. All data that was
    created by ourselves are discriminated in
    “data/dataset/RawFilesList.csv”.

### 4.2. ER diagram

Entity-relationship diagram of “data/dataset” folder.

<figure>
<img src="data/metadata/dataModel_MySQL/ModeloDados.png"
alt="Entity-relationship diagram" />
<figcaption aria-hidden="true">Entity-relationship diagram</figcaption>
</figure>

Original MYSQL Workbench file in “data/metadata/dataModel_MySQL/”.

### 4.3. Final Dataset

Final dataset, “data/analysisdata/DataAnalysis.csv”, derived from all
raw data above. It includes muncipality-year values for all Brazilian
municipalities that had above 50.0000 inhabintants in 1996. The
variables are detailes in the “data/dataset/Codebook.csv”.

## 5. Folders and Files

### 5.1. Folders descriptions

The main folders are:

- **data**
- **doc**
- **src**

Inside the “data” folder (and its subfolders) are both raw data and
clean and transformed data. There is also all the metadata used in this
project. In the “doc” folder are the paper itself, as well as figures,
tables and presentations. Within the “src” folder are all the codes and
code functions used in the project.

#### 5.1.1 Data folder description <a name="datafolder"></a>

The complete structure of the “data” folder is the following:

- data
  - analysisdata
  - dataset
  - metadata
    - dataModel_MySQL
      - History
    - documentation
  - raw
    - CepespData
      - .cepespdata
    - Finbra
      - ExcelFiles
      - OriginalFiles
    - PBCENSUS Spada
    - TSE
      - 2000
      - TSE-relacao-candidatos-1996
  - temp

Folders description:

- **data/analysisdata:** This folder contains the final dataset, which
  is a data report containing a mixt of modeled data and indicators
  (transformed data) used to run the project’s statistical models,
  tables and figures.

- **data/dataset:** Data extracted from the raw sources into a tidy
  relational base model. All data used to create final dataset variables
  are extracted from the files in this folder. This folder also have
  most of metadata that computacional scripts uses to create the
  database. All data are in csv format.

- **data/metadata:** Gathers part of the documentation about the data.
  The metadata that are in dataset format are in “data/dataset” folder.
  This folder contains metadata that are in other formats, like MySQL
  Workbench files or text files.

- **data/metadata/dataModel_MySQL:** MySQL Workbench files that show the
  entity-relationship diagram of the dataset in “data/dataset” folder.
  It contais a “History” folder that shows the evolution of the data
  model in this project.

- **data/metadata/documentation:** Codebook and other documentation
  about dataset variables.

- **data/raw:** Stores most of the downloaded raw data, like TSE,
  STN-Finbra and PBCENSUS data. Data that came from CEPESP API, IPEA API
  or is result of our own research was inserted directly in
  “data/dataset”. There is also a directory for storing cache data of
  CEPESP API (ommited in github), to avoid redundant downloads. Mode
  detail about raw data in [Data sources section](#datasources)

- **data/temp:** Directory for temporary data storage

#### 5.1.2 Documments folder description

The complete structure of the “doc” folder is the following:

- doc
  - figures
  - paper
  - presentations
  - tables

The content of each folder is pretty intuitive, containing figures, text
of the paper, authors presentations and tables of the paper.

#### 5.1.2 Scripts folder description <a name="scriptsfolder"></a>

The complete structure of the “src” folder is the following:

- src
  - generalFunctions
  - specifcFuntions

Folders description:

- **src/:** Gathers all the programming codes (only R language was
  used). In this folder are the scripts that call the other functions.

- **src/generalFunctions:** Here are very general functions that
  probably will be used by the authors in other projects.

- **src/specifcFuntions:** Here are functions for the specif extraction
  of the data used in this project. Mode detail about codes mechanics in
  [Code description section](#codedescription)

### 5.2. Files Description and File Dependence

The description of the role of each files in this project are in
“data/dataset/FileList.csv”. We also recommend a careful review of
metadata files “data/dataset/FileTracing.csv”, that describes the
interdependence between codes and data files and the
“data/dataset/Codebook.csv” that describes all variables present in data
and metadata files.
