library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
library(furrr)
source("R/utils.R")

future::plan(multisession)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### Global Variables ##############
####################################
GIT_PATH <- "~/git_token.txt"
TAP_TBL <- "syn10374665"
FILE_COLUMNS <- c("tapping_results.json.TappingSamples")
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_PATH <- file.path("R", "extract_tapping_v1_tables.R")
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = SCRIPT_PATH)
OUTPUT_PARENT_ID <- "syn25756375"
OUTPUT_FILE <- "mhealthtools_tapping_features_mpowerV1.tsv"

parse_tap_samples <- function(data){
    if(is.null(ncol(data))){
        tb <- tibble(t = double(),
                     x = double(),
                     y = double(),
                     buttonid = character())
        return(tb)
    }
    tap_coord <- do.call(
        rbind, map(
            data$TapCoordinate, 
            function(x)stringr::str_extract_all(x, "[[:digit:]]+\\.*[[:digit:]]*")[[1]])) %>% 
        as_tibble() %>% 
        select(x = V1, y = V2) %>% 
        mutate_at(vars(x,y), as.double)
    data <- cbind(data, tap_coord) %>% 
        select(t = TapTimeStamp, x, y, buttonid= "TappedButtonId")
    return(data)
}


process_tapping_samples <- function(filePath){
    tryCatch({
        data <- fromJSON(filePath)
        if(nrow(data) == 0){
            stop("error: empty files")
        }
        data %>% 
            parse_tap_samples(.) %>%
            mhealthtools::get_tapping_features(.)
    }, error = function(err){
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(geterrmessage(), "\n", ""))
        return(tibble::tibble(error = error_msg))
    })
}

parallel_process_tapping_samples <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            fileColumnName = data$fileColumnName,
                                            filePath = data$filePath), function(recordId, 
                                                                                fileColumnName, 
                                                                                filePath){
                                                process_tapping_samples(filePath) %>% 
                                                    dplyr::mutate(recordId = recordId,
                                                                  fileColumnName = fileColumnName) %>%
                                                    dplyr::select(recordId, fileColumnName, everything())})
    data %>% 
        dplyr::select(all_of(c("recordId", "fileColumnName", KEEP_METADATA))) %>%
        dplyr::inner_join(features, by = c("recordId", "fileColumnName"))
}

save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        "extract tap features for mPower V1", 
        executed = GIT_URL,
        used = c(TAP_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}

main <-  function(){
    data <- get_table(syn = syn, synapse_tbl = TAP_TBL,
              file_columns = FILE_COLUMNS,
              uid = UID, keep_metadata = KEEP_METADATA) %>%
        parse_medTimepoint() %>%
        parse_phoneInfo() %>%
        parallel_process_tapping_samples() %>% 
        save_to_synapse()
}

main()
