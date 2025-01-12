#' Download CNEFE from Brazil's census
#'
#' @description
#' Download microdata of population addresses from Brazil's census.
#'
#' @template year
#' @template columns
#' @template as_data_frame
#' @template showProgress
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#' @export
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' # return data as arrow Dataset
#' df <- read_cnefe(year = 2022, code_state=29,
#'                       showProgress = FALSE)
#'
#'
read_cnefe <- function(code_state=29,
                       year = 2022,
                       columns = NULL,
                       as_data_frame = FALSE,
                       showProgress = TRUE,
                       cache = TRUE) {

  ### check inputs
  checkmate::assert_numeric(year)
  checkmate::assert_numeric(code_state)
  checkmate::assert_logical(as_data_frame)

  # data available for the years:
  years <- c(2022)
  if (isFALSE(year %in% years)) { stop(paste0("Error: Data currently only available for the years ",
                                             paste(years, collapse = " ")))}

  code_states <- c(11, 28, 29, 31)
  if (isFALSE(code_state %in% code_states)) { stop(paste0("Error: Data currently only available for the state code ",
                                              paste(code_states, collapse = " ")))}

  ### Get url
  file_url <- paste0(censobr_env$censobr_release_url,
                     censobr_env$data_release, "/", year, "_cnefe_", code_state,"_",
                     censobr_env$data_release, ".parquet")


  ### Download
  local_file <- download_file(file_url = file_url,
                              showProgress = showProgress,
                              cache = cache)

  # check if download worked
  if(is.null(local_file)) { return(NULL) }


  ### read data
  df <- arrow_open_dataset(local_file)

  ### Select
  if (!is.null(columns)) { # columns <- c('V0002','V0011')
    df <- dplyr::select(df, dplyr::all_of(columns))
  }

  ### output format
  if (isTRUE(as_data_frame)) { return( dplyr::collect(df) )
  } else {
    return(df)
  }

}

