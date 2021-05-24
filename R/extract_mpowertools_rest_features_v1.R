########################################################################
#' Utility script for extracting pdkit features
#' 
#' Purpose: 
#' This script is used to extract
#' walking features (PDKit + Rotation) fron 30s walk data
#' 
#' Author: Aryton Tediarjo
#' email: aryton.tediarjo@sagebase.org
########################################################################
library(reticulate)
library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(argparse)
library(furrr)
library(mpowertools)
source("R/utils.R")

#################################
# Python objects
################################
future::plan(multicore)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()


#################################
# Global Variables
################################
WALK_TBL <- "syn10308918"
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
OUTPUT_FILE <- "pdkit_rotation_features_mpower_v1_freeze.tsv"
OUTPUT_PARENT_ID <- "syn25756375"
FILE_COLUMNS <- c("accel_walking_rest.json.items")
UID <- c("recordId")
SCRIPT_PATH <- file.path("R", "extract_mpowertools_rest_features_v1_table.R")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
ACTIVITY_NAME <- "extract mpower v1 mpowertools features using accel"

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)


process_rest_samples <- function(filePath){
    #' Function to shape time series from json
    #' and make it into the valid formatting
    features <- tryCatch({
        data <- fromJSON(filePath)
        if(nrow(data) == 0){
            stop("error: empty files")
        }
        return(mpowertools:::GetBalanceFeatures(data) %>% 
                   purrr::map_dfr(function(feature){feature}))
    }, error = function(err){
        error_msg <- "Can't process time-series"
        return(tibble::tibble(error = c(error_msg)))
    })
    return(features)
}

parallel_process_rest_samples <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            fileColumnName = data$fileColumnName,
                                            filePath = data$filePath), function(recordId, 
                                                                                fileColumnName, 
                                                                                filePath){
                                                process_rest_samples(filePath) %>% 
                                                    dplyr::mutate(recordId = recordId,
                                                                  fileColumnName = fileColumnName) %>%
                                                    dplyr::select(recordId, fileColumnName, everything())})
    data %>% 
        dplyr::select(all_of(c("recordId", "fileColumnName", KEEP_METADATA))) %>%
        dplyr::inner_join(features, by = c("recordId", "fileColumnName")) %>% 
        dplyr::mutate(across(!c(UID, KEEP_METADATA, "error", "fileColumnName"), as.double))
}

save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        name = ACTIVITY_NAME, 
        executed = GIT_URL,
        used = c(WALK_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}


main <- function(){
    #' get raw data
    raw_data <- get_table(syn = syn, 
                          synapse_tbl = WALK_TBL,
                          file_columns = FILE_COLUMNS,
                          uid = UID, 
                          keep_metadata = KEEP_METADATA) %>% 
        parse_phoneInfo() %>%
        parse_medTimepoint() %>%
        parallel_process_rest_samples() %>%
        save_to_synapse()
}

main()

