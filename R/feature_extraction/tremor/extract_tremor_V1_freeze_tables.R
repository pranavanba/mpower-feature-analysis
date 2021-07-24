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
TREMOR_TBL <- "syn10676309"
FILE_COLUMNS <- c("accel_tremor_handInLap_right.json.items", 
                  "deviceMotion_tremor_handInLap_right.json.items",
                  "accel_tremor_handInLap_left.json.items", 
                  "deviceMotion_tremor_handInLap_left.json.items"
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

process_tremor_samples <- function(accel, deviceMotion){
    #' Function to shape time series from json
    #' and make it into the valid formatting
    tryCatch({
        accel_ts <- parse_accel_data(accel)
        rotation_ts <- parse_rotation_data(deviceMotion)
        if(nrow(accel_ts) == 0 | nrow(rotation_ts) == 0){
            stop("error: empty time-series")
        }else{
            features <- mhealthtools::get_tremor_features(
                accelerometer_data = accel_ts,
                gyroscope_data = rotation_ts,
                time_filter = c(5,25), 
                window_length = 256,
                window_overlap = 0.25,
                frequency_filter = c(3, 15),
                detrend = TRUE,
                derived_kinematics = TRUE,
                funs = c(mhealthtools::time_domain_summary,
                         mhealthtools::frequency_domain_summary))
            if (is.null(features$error) && !is.null(features$extracted_features)) {
                return(features$extracted_features %>%
                           dplyr::mutate(error = NA))
            } else {
                return(tibble::tibble(
                    error = as.character(stringr::str_c(features$error)) %>%
                        str_replace_all("[[:punct:]]", "")))
            }
        }
    }, error = function(err){ # capture all other error
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(as.character(geterrmessage()), 
                                     "[[:punct:]]", ""))
        return(tibble::tibble(error = error_msg))
    })
}

parallel_process_samples <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            activityType = data$activityType,
                                            accel = data$accel,
                                            deviceMotion = data$deviceMotion), 
                                       function(recordId, activityType, accel, deviceMotion){
                                           process_tremor_samples(accel, deviceMotion) %>% 
                                               dplyr::mutate(recordId = recordId,
                                                             activityType = activityType) %>%
                                               dplyr::select(recordId, activityType, everything())})
    
    data %>% 
        dplyr::select(all_of(c("recordId", "activityType"))) %>%
        dplyr::inner_join(features, by = c("recordId", "activityType"))
}


main <- function(){
    #' get raw data
    tremor_features <- get_table(syn = syn, 
                                 synapse_tbl = TREMOR_TBL,
                                 download_file_columns = FILE_COLUMNS) %>% 
        dplyr::select(recordId, fileColumnName, filePath) %>%
        dplyr::mutate(
            sensorType = ifelse(str_detect(
                fileColumnName, "^deviceMotion"), 
                "deviceMotion", "accel"),
            activityType = ifelse(str_detect(fileColumnName, "left"), 
                                  "left_hand_tremor", "right_hand_tremor")) %>%
        tidyr::pivot_wider(
            id_cols = c(recordId, activityType),
            names_from = "sensorType", 
            values_from = "filePath") %>%
        parallel_process_samples() %>% 
        save_to_synapse(syn = syn,
                        synapseclient = synapseclient,
                        data = .,
                        output_filename = OUTPUT_FILE,
                        parent = OUTPUT_PARENT_ID,
                        executed = GIT_URL,
                        used = TREMOR_TBL)
}

main()