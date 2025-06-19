FROM r-base:latest

RUN apt-get update && apt-get install -y \
        unixodbc \
        unixodbc-dev \
        odbc-postgresql

RUN apt-get install -y libpq-dev

WORKDIR /app

COPY ./src/.env  /app/
COPY ./src/pipeline_tb_projeto.R  /app/

RUN R -e "install.packages(c('data.table', 'DBI', 'dotenv', 'RPostgres'), repos='https://cran.r-project.org')"
RUN R -e "install.packages('odbc', repos='https://cran.r-project.org')"


CMD ["Rscript", "pipeline_tb_projeto.R"]
