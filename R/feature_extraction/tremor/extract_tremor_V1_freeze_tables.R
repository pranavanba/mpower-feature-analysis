library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
library(furrr)
source("R/utils/reticulate_utils.R")

future::plan(multisession)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

####################################
#### Global Variables ##############
####################################
GIT_PATH <- "~/git_token.txt"
TREMOR_TBL <- "syn12977322"
FILE_COLUMNS <- c("accel_tremor_handInLap_right.json.items", 
                  "deviceMotion_tremor_handInLap_right.json.items",
                  "accel_tremor_handInLap_left.json.items", 
                  "deviceMotion_tremor_handInLap_left.json.items",
                  )
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
NAME <- "extract tremor features"
PARALLEL <- TRUE

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_PATH <- 'R/feature_extraction/tremor/extract_tremor_V1_freeze_tables.R'
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = SCRIPT_PATH)
OUTPUT_PARENT_ID <- "syn25756375"
OUTPUT_FILE <- "mhealthtools_tremor_features_mpowerV1_freeze.tsv"

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)

###########################
#### helper functions ####
###########################
parse_accel_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1]) %>%
        dplyr::select(t = timestamp, x, y, z)
}

parse_rotation_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(t = timestamp - .$timestamp[1],
                      x = .$rotationRate$x,
                      y = .$rotationRate$y,
                      z = .$rotationRate$z) %>% 
        dplyr::select(t, x, y, z)
}

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