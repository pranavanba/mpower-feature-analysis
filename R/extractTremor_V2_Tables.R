library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
library(plyr)
library(doMC)
source("R/utils.R")

doMC::registerDoMC(parallel::detectCores())
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### Global Variables ##############
####################################
GIT_PATH <- "~/git_token.txt"
TREMOR_TBL <- "syn12977322"
FILE_COLUMNS <- c("right_motion.json", "left_motion.json")
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
NAME <- "extract tremor (time-domain summary) features"
PARALLEL <- TRUE

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_NAME <- "extractTremor_V2_Tables.R"
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = 'R/extractTremor_V2_Tables.R')

OUTPUT_PARENT_ID <- "syn25691532"
OUTPUT_FILE <- "mhealthtools_tremor_features_mpowerV2.tsv"


search_gyro_accel <- function(ts){
    #' split to list
    ts_list <- list()
    ts_list$acceleration <- ts %>% 
        dplyr::filter(
            stringr::str_detect(tolower(sensorType), "^accel"))
    ts_list$rotation <- ts %>% 
        dplyr::filter(
            stringr::str_detect(tolower(sensorType), "^rotation|^gyro"))
    
    #' parse rotation rate only
    if(length(ts_list$rotation$sensorType %>% unique(.)) > 1){
        ts_list$rotation <- ts_list$rotation %>% 
            dplyr::filter(!stringr::str_detect(tolower(sensorType), "^gyro"))
    }
    
    ts_list <- ts_list %>%
        purrr::map(., ~(.x %>%
                            dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
                            dplyr::select(t,x,y,z)))
    return(ts_list)
}



#' Entry-point function to parse each filepath of each recordIds
#' walk data and featurize each record using featurize walk data function
#' @params data: dataframe containing filepaths
#' @returns featurized walk data for each participant 
process_tremor_samples <- function(filePath){
    tryCatch({ # capture common errors
        ts <- jsonlite::fromJSON(filePath)
        if(nrow(ts) == 0){
            stop("ERROR: sensor timeseries is empty")
        }else{
            ts_list <- ts %>% search_gyro_accel()
            features <- mhealthtools::get_tremor_features(
                accelerometer_data = ts_list$acceleration,
                gyroscope_data = ts_list$rotation,
                time_filter = c(5,25), 
                window_length = 100,
                window_overlap = 0.25,
                frequency_filter = c(3, 15),
                detrend = TRUE,
                funs = c(mhealthtools::time_domain_summary))
            if (is.null(features$error) && !is.null(features$extracted_features)) {
                return(features$extracted_features %>%
                           dplyr::mutate(error = NA))
            } else {
                return(tibble::tibble(error = features$error))
            }
        }
    }, error = function(err){ # capture all other error
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(geterrmessage(), "\n", ""))
        return(tibble::tibble(error = error_msg))
    })
}

parallel_process_tremor_features <- function(data){
    data %>%
        plyr::ddply(
            .variables = c("recorId", "fileColumnName", KEEP_METADATA),
            .fun = function(row){process_tremor_samples(row$filePath)})
}


save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        name = NAME, 
        executed = GIT_URL,
        used = c(TREMOR_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}

main <- function(){
    #' get raw data
    tremor_features <- get_table(syn = syn, synapse_tbl = TREMOR_TBL,
                               file_columns = FILE_COLUMNS,
                               uid = UID, keep_metadata = KEEP_METADATA) %>% 
        parse_medTimepoint() %>%
        parse_phoneInfo() %>%
        parallel_process_tremor_features() %>% 
        save_to_synapse()
}

main()