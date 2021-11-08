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
source("R/utils.R")

#################################
# Python objects
################################
synapseclient <- reticulate::import("synapseclient")
pdkit_rotation_features <- reticulate::import("PDKitRotationFeatures")
gait_feature_objs <- pdkit_rotation_features$gait_module$GaitFeatures(sensor_window_size = 750L)
syn <- synapseclient$login()


#################################
# Global Variables
################################
WALK_TBL <- "syn10308918"
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
OUTPUT_FILE <- "pdkit_rotation_features_mpower_v1_freeze.tsv"
OUTPUT_PARENT_ID <- "syn25756375"
FILE_COLUMNS <- c("accel_walking_outbound.json.items",
                "deviceMotion_walking_outbound.json.items",
                "accel_walking_return.json.items",
                "deviceMotion_walking_return.json.items",
                "accel_walking_rest.json.items",
                "deviceMotion_walking_rest.json.items")
UID <- c("recordId")
SCRIPT_PATH <- file.path("R", "extract_pdkit_rotation_walk_features_v1_table.R")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
ACTIVITY_NAME <- "extract mpower v1 features using accel"

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)

###########################
#### helper functions ####
##########################
process_walk_samples <- function(accel, deviceMotion, feature_obj){
    #' Function to shape time series from json
    #' and make it into the valid formatting
    features <- tryCatch({
        accel_ts <- parse_accel_data(accel)
        rotation_ts <- parse_rotation_data(deviceMotion)
        if(nrow(accel_ts) == 0 | nrow(rotation_ts) == 0){
            stop("error: empty time-series")
        }
        return(gait_feature_objs$run_pipeline(accel_ts, rotation_ts) %>%
                   dplyr::mutate(error = as.character(error)))
    }, error = function(err){
        error_msg <- "Can't process time-series"
        return(tibble::tibble(error = c(error_msg)))
    })
    return(features %>% 
               dplyr::mutate(error = as.character(error)))
}

parallel_process_walk_samples <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            activityType = data$activityType,
                                            accel = data$accel,
                                            deviceMotion = data$deviceMotion), 
                                       function(recordId, activityType, accel, deviceMotion, feature_obj){
                                                process_walk_samples(accel, deviceMotion, feature_obj) %>% 
                                                    dplyr::mutate(recordId = recordId,
                                                                  activityType = activityType) %>%
                                                    dplyr::select(recordId, activityType, everything())},
                                       feature_obj = gait_feature_objs)
    
    data %>% 
        dplyr::select(all_of(c("recordId", "activityType", KEEP_METADATA))) %>%
        dplyr::inner_join(features, by = c("recordId", "activityType"))
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
        dplyr::mutate(sensorType = ifelse(str_detect(fileColumnName, "^deviceMotion"), "deviceMotion", "accel"),
                      activityType = case_when(str_detect(fileColumnName, "outbound") ~ "outbound",
                                               str_detect(fileColumnName, "return") ~ "return",
                                               TRUE ~ "rest")) %>% 
        dplyr::select(all_of(UID), 
                      all_of(KEEP_METADATA), 
                      activityType, 
                      sensorType, filePath) %>%
        pivot_wider(names_from = sensorType, 
                    values_from = filePath) %>%
        parallel_process_walk_samples() %>%
        save_to_synapse()
}

main()

