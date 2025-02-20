library(piggyback)



parquet_files <- list.files('./data/',
                            full.names = T,
                            recursive = T, pattern = '_v0.3.0.1.parquet')

piggyback::pb_upload(file = parquet_files,
                     repo = 'leoniedu/censobr',
                     tag = 'v0.3.0.1'
                     , overwrite = TRUE
                     )
