# Teste para inserir dado de geolocalização na base (PostGIS do Postgres)

# By Murilo Junqueira (m.junqueira@yahoo.com.br)

# Created at 2024-03-01

library(tidyverse)
library(data.table)
library(stringr)
library(DBI)
library(RODBC)
library(RPostgres)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conecta à base ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Baixa a chave secreta do código
assert_that(file.exists("keys/psql12-homolog_key.json"))
keys <- jsonlite::read_json("keys/psql12-homolog_key.json")


# Verifica se pode conectar
TestConexao <- dbCanConnect(RPostgres::Postgres(), 
                            dbname = keys$dbname,
                            host = keys$host,
                            port = keys$port,
                            user = keys$username, 
                            # options="-c search_path=osc"
                            password = keys$password)

assert_that(TestConexao, 
            msg = paste("O teste de coneção falhou, ", 
                        "verificar nome da base, host, ",
                        "porta, usuário e senha."))

# conencta à base
connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = keys$dbname,
                    host = keys$host,
                    port = keys$port,
                    user = keys$username, 
                    # options="-c search_path=osc",
                    password = keys$password)

# Verifica a coneção com a base
assert_that(dbIsValid(connec))

rm(keys, TestConexao)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Cria um dado ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Testa extrair dados do banco
Teste <- try(dbGetQuery(connec, 
                        paste0("SELECT * FROM tb_localizacao",
                               # " LIMIT 500", 
                               ";")))

id <- which(Teste$id_osc == 1155032)


Teste$geo_localizacao[id] 
"POINT (-49.6295582109996 -25.5808608209996)"
# "010100002042120000F5950B5D95D048C093B8754BB39439C0"

Teste$tx_endereco[id] # "RODOVIA PR 510"

# rm(id)

Teste_n1 <- Teste[id, ] %>% 
  mutate(temp_var = NA_character_, 
         lat = NA_real_, 
         lon = NA_real_) %>% 
  add_row(id_osc = 01, 
          temp_var = "POINT (-49.6295582109996 -25.5808608209996)", 
          lat = -49.6295582109996, 
          lon = -25.5808608209996)


names(Teste_n1)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Estratégia 2: Formatar previamente a tabela ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tb_localizacao <- readRDS("backup_files/2023_01/output_files/tb_localizacao.RDS")
names(tb_localizacao)

Teste_n2 <- tb_localizacao %>% 
  select(id_osc, tx_latitude, tx_longitude) %>% 
  dplyr::filter(!is.na(tx_latitude)) %>% 
  mutate(tx_gem = paste0("POINT (", 
                         tx_longitude, " ", 
                         tx_latitude, 
                         ")" 
                         ), 
         nr_latitude = as.numeric(tx_latitude), 
         nr_longitude = as.numeric(tx_longitude) )

if(dbExistsTable(connec, "teste_geo")) dbRemoveTable(connec, "teste_geo")
dbWriteTable(connec, "teste_geo", Teste_n2)

dbExistsTable(connec, "teste_geo")


dbGetQuery(connec, "SELECT CURRENT_SCHEMA, CURRENT_SCHEMA();")
dbGetQuery(connec, "SELECT PostGIS_Full_Version();")


x <- dbExecute(connec, "ALTER TABLE teste_geo ADD COLUMN geom geometry(point, 4674)")

check <- dbGetQuery(connec, paste0("SELECT * FROM teste_geo",
                                   " LIMIT 5000", 
                                   ";"))

x <- dbGetQuery(connec, "UPDATE teste_geo SET geom = ST_SetSRID(ST_MakePoint(nr_longitude, nr_latitude), 4674);")
x

check <- dbGetQuery(connec, paste0("SELECT * FROM teste_geo",
                                   " LIMIT 5000", 
                                   ";"))


UpdateQuery <- "UPDATE teste_geo SET geom = ST_GeomFromText('POINT (-49.6295582109996 -25.5808608209996)', 4674)"

library(rpostgis)
pgPostGIS(connec)




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Estratégia 1: Otimismo - basta configurar o campo e mandar no formato texto ####
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Cria nova tabela:
if(dbExistsTable(connec, "teste_geo")) dbRemoveTable(connec, "teste_geo")
dbWriteTable(connec, "teste_geo", Teste_n1)

dbExistsTable(connec, "teste_geo")


# Pega o tipo de variável a ser criado:
QueryGetInformation <- paste0("SELECT *", "\n",
                              "FROM information_schema.columns", "\n",
                              "WHERE table_name = 'teste_geo';")
cat(QueryGetInformation)
ColTypes <- dbGetQuery(connec, QueryGetInformation)


ColTypes$column_name

VarType <- ColTypes$data_type[ColTypes$column_name == "geo_localizacao"]

UpdateQuery <- "UPDATE teste_geo SET geom = ST_GeomFromText('POINT (-49.6295582109996 -25.5808608209996)', 4674)"

dbExecute(connec, UpdateQuery)


debug1 <- dbGetQuery(connec, "SELECT PostGIS_Full_Version();")
debug1

debug1 <- paste0("CREATE postgis")

install.packages("rpostgis")


pgMakePts(conn = connec,
          name = c("osc", "teste_geo"), 
          colname = "geom", 
          x = "lon", 
          y = "lat", 
          srid = 4674)

dbExecute(connec, "ALTER TABLE teste_geo ADD COLUMN geom geometry(point, 4674)")

QueryaddGeom <- "ALTER TABLE teste_geo, ADD COLUMN geom geometry(Point, 4674)"


dbExecute(connec, "SELECT postgis_full-version();")

x <- dbGetQuery(connec, "SELECT CURRENT_SCHEMA, CURRENT_SCHEMA();")

x


x <- dbGetQuery(connec, "SELECT ST_MakePoint(-50.3482090039996,-20.7619611619996);")
x

x <- dbGetQuery(connec, "select point('010100002042120000EE2CD61C922C49C0469CFFE20FC334C0');")

x <- dbGetQuery(connec, "SELECT ST_X('010100002042120000EE2CD61C922C49C0469CFFE20FC334C0') as latitude, ST_Y('010100002042120000EE2CD61C922C49C0469CFFE20FC334C0') AS longitude;")
x

x <- dbGetQuery(connec, "SELECT ST_GeomFromText('POINT (-49.6295582109996 -25.5808608209996)', 4674)")
x




rpostgis::pgPostGIS(connec)


library(sf)
pts <- st_sf(a = 1:2, geom = st_sfc(st_point(0:1), st_point(1:2)), crs = 4326)

## Insert data in new database table
pgWriteGeom(connec, name = c("osc", "teste_geo"), data.obj = pts, partial.match = TRUE)


?rpostgis::pgWriteGeom()


# Desconecta da base
dbDisconnect(connec)

rm(connec)

