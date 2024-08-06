library(dplyr)
library(arrow)

## download
cachedir <- censobr:::censobr_env$cache_dir


# [ICO]	Name	Last modified	Size	Description
# [PARENTDIR]	Parent Directory	 	-
#   [DIR]	Agregados_por_CEP/	2024-06-14 10:00	-
#   [DIR]	Arquivos_CNEFE/	2024-05-20 19:51	-
#   [DIR]	Coordenadas_enderecos/ 2024-01-31 16:29

# The latest is Arquivos_CNEFE
# So we will get those by UF

ftp <- "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/UF/"

h <- rvest::read_html(ftp)
elements <- rvest::html_elements(h, "a")
files <- rvest::html_attr(elements, "href")
filenameext <- files[ data.table::like(files, '.zip') ]
year <- 2022
timeout_ <- 60*30
# Download zipped files
dest_dir <- paste0('./data_raw/cnefe/', year)
dest_files <- file.path(dest_dir, filenameext)
res <- curl::multi_download(paste0(ftp, filenameext),
                            destfiles = dest_files,
                            progress = TRUE, resume = TRUE,
                            timeout = timeout_, multiplex = TRUE, accept_encoding = c("gzip,deflate"), ssl_verifypeer=FALSE)
## might need to repeat!


## open csv dataset
library(readr)
col_desc <- cols(
  COD_UNICO_ENDERECO = col_character(),
  COD_UF = col_character(),
  COD_MUNICIPIO = col_character(),
  COD_DISTRITO = col_character(),
  COD_SUBDISTRITO = col_character(),
  COD_SETOR = col_character(),
  NUM_QUADRA = col_character(),
  NUM_FACE = col_character(),
  CEP = col_character(),
  DSC_LOCALIDADE = col_character(),
  NOM_TIPO_SEGLOGR = col_character(),
  NOM_TITULO_SEGLOGR = col_character(),
  NOM_SEGLOGR = col_character(),
  NUM_ENDERECO = col_double(),
  DSC_MODIFICADOR = col_character(),
  NOM_COMP_ELEM1 = col_character(),
  VAL_COMP_ELEM1 = col_character(),
  NOM_COMP_ELEM2 = col_character(),
  VAL_COMP_ELEM2 = col_character(),
  NOM_COMP_ELEM3 = col_character(),
  VAL_COMP_ELEM3 = col_character(),
  NOM_COMP_ELEM4 = col_character(),
  VAL_COMP_ELEM4 = col_character(),
  NOM_COMP_ELEM5 = col_character(),
  VAL_COMP_ELEM5 = col_character(),
  LATITUDE = col_double(),
  LONGITUDE = col_double(),
  NV_GEO_COORD = col_character(),
  COD_ESPECIE = col_character(),
  DSC_ESTABELECIMENTO = col_character(),
  COD_INDICADOR_ESTAB_ENDERECO = col_character(),
  COD_INDICADOR_CONST_ENDERECO = col_character(),
  COD_INDICADOR_FINALIDADE_CONST = col_character(),
  COD_TIPO_ESPECI = col_character()
)
lookup_cnefe <- c(code_unique_address = "COD_UNICO_ENDERECO", code_state = "COD_UF",
                  code_muni = "COD_MUNICIPIO", code_district = "COD_DISTRITO",
                  code_subdistrict = "COD_SUBDISTRITO", code_tract = "COD_SETOR",
                  num_quadra = "NUM_QUADRA", num_face = "NUM_FACE", cep = "CEP",
                  dsc_localidade = "DSC_LOCALIDADE", name_tipo_seglogr = "NOM_TIPO_SEGLOGR",
                  name_titulo_seglogr = "NOM_TITULO_SEGLOGR", name_seglogr = "NOM_SEGLOGR",
                  num_endereco = "NUM_ENDERECO", dsc_modificador = "DSC_MODIFICADOR",
                  name_comp_elem1 = "NOM_COMP_ELEM1", val_comp_elem1 = "VAL_COMP_ELEM1",
                  name_comp_elem2 = "NOM_COMP_ELEM2", val_comp_elem2 = "VAL_COMP_ELEM2",
                  name_comp_elem3 = "NOM_COMP_ELEM3", val_comp_elem3 = "VAL_COMP_ELEM3",
                  name_comp_elem4 = "NOM_COMP_ELEM4", val_comp_elem4 = "VAL_COMP_ELEM4",
                  name_comp_elem5 = "NOM_COMP_ELEM5", val_comp_elem5 = "VAL_COMP_ELEM5",
                  latitude = "LATITUDE", longitude = "LONGITUDE", nv_geo_coord = "NV_GEO_COORD",
                  code_especie = "COD_ESPECIE", dsc_estabelecimento = "DSC_ESTABELECIMENTO",
                  code_indicador_estab_endereco = "COD_INDICADOR_ESTAB_ENDERECO",
                  code_indicador_const_endereco = "COD_INDICADOR_CONST_ENDERECO",
                  code_indicador_finalidade_const = "COD_INDICADOR_FINALIDADE_CONST",
                  code_tipo_especi = "COD_TIPO_ESPECI")

process_batch <- function(data, pos) {
  j <<- j+1
  data%>%
    dplyr::rename(any_of(lookup_cnefe))%>%
    mutate(partition=j)%>%
    group_by(code_state, partition)%>%
    #write_dataset('data_raw/cnefe/2022/')
    write_dataset(file.path(tempdir))
}

library(readr)
tempdir <- file.path(tempdir(), "cnefe", '2022')
unlink(tempdir, recursive = TRUE)
for (k in dest_files) {
  print(k)
  j <<- 0
  ## read zip, write temp file
  cnefe_in <- readr::read_delim_chunked(file=k, col_types = col_desc, delim = ";", locale = locale(decimal_mark = "."), chunk_size = 1e6, callback = SideEffectChunkCallback$new(process_batch))
}

## write final files
fdir <- file.path(censobr:::censobr_env$cache_dir, "cnefe", "2022")
states <- open_dataset(tempdir)%>%
  ungroup()%>%
  distinct(code_state)%>%
  collect()%>%
  pull(code_state)


#dir.create(file.path("data/cnefe/", year), recursive = TRUE)
for (j in states) {
  print(j)
  open_dataset(tempdir)%>%
    filter(code_state==j)%>%
    ungroup()%>%
    select(-partition)%>%
    write_parquet(file.path("data", paste0(year, "_cnefe_", j,"_v0.3.0.1.parquet")))
}


# open_dataset(tempdir)%>%
#   group_by(code_state)%>%
#   select(-partition)%>%
#   #write_dataset(fdir)
# unlink(tempdir, recursive = TRUE)
#
# ## read final file
# res <- open_dataset(fdir)
# res%>%count(code_state=as.character(code_state))%>%arrange(-n)%>%collect()
#
#
# # r1 <- gsub("^cod_", "code_", tolower(names(cnefe_y)))
# # r2 <- gsub("^distrito", "district", r1)
# # r3 <- gsub("^nom_", "name_", r2)
# # lookup_cnefe <- names(cnefe_y)
# # names(lookup_cnefe) <- r3
