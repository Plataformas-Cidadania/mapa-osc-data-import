# -----------------------------
# 1. Base e dependências do sistema
# -----------------------------
FROM r-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
        unixodbc \
        unixodbc-dev \
        odbc-postgresql \
        libpq-dev \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# -----------------------------
# 2. Instalação de pacotes R
# -----------------------------
RUN R -e "install.packages(c('data.table', 'DBI', 'dotenv', 'RPostgres', 'odbc'), repos='https://cran.r-project.org')"

# -----------------------------
# 3. Configuração da aplicação
# -----------------------------
WORKDIR /app

COPY ./src ./

# -----------------------------
# 4. Execução do script
# -----------------------------
CMD ["Rscript", "pipeline_tb_projeto.R"]        