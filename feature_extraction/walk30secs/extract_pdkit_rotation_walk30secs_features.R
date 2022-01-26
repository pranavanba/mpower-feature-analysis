###########################################################
#' Utility function to extract tapping (parallel or sequentially)
#' features across mPowerV1 and mPowerV2 interchangably
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
library(plyr)
library(doMC)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
source("utils/reticulated_fetch_id_utils.R")

#' Get Synapse Creds
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()
syn$table_query_timeout <- 9999999
pdkit_rotation_features <- reticulate::import("PDKitRotationFeatures")

#' Global Variables
MPOWER_VERSION <- Sys.getenv("R_CONFIG_ACTIVE")
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$walk,
    feature_extraction = get_feature_extraction_ids(syn = syn))
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
WALK_TABLE <- SYN_ID_REF$table
SCRIPT_PATH <- file.path(
    "feature_extraction", 
    "walk30secs",
    "extract_pdkit_rotation_walk30secs_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)

#' Function to format V1 time-series
format_v1_time_series <- function(accel_filePath, rotation_filePath){
    accel <- jsonlite::fromJSON(accel_filePath) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1]) %>%
        dplyr::select(t = timestamp, x, y, z) %>%
        dplyr::mutate(sensor = "acceleration")
    rotation <- jsonlite::fromJSON(rotation_filePath) %>%
        dplyr::mutate(t = timestamp - .$timestamp[1],
                      x = .$rotationRate$x,
                      y = .$rotationRate$y,
                      z = .$rotationRate$z) %>% 
        dplyr::select(t, x, y, z) %>%
        dplyr::mutate(sensor = "rotation")
    dplyr::bind_rows(accel, rotation)
}

#' Function to format V2 data
format_v2_time_series <- function(file_path, sensor_pattern){
    file_path %>%
        jsonlite::fromJSON()  %>%
        dplyr::filter(stringr::str_detect(sensorType, sensor_pattern)) %>%
        dplyr::group_by(sensorType) %>%
        tidyr::nest() %>%
        dplyr::slice(1) %>%
        tidyr::unnest(data) %>% 
        dplyr::ungroup() %>%
        dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
        dplyr::select(t, x, y, z)
}

#' Function to extract gyro/accel    
search_gyro_accel <- function(filePath){
    accel <- filePath %>% 
        format_v2_time_series("^accel") %>%
        dplyr::mutate(sensor = "acceleration")
    rotation <- filePath %>% 
        format_v2_time_series("^gyro|^rotation") %>%
        dplyr::mutate(sensor = "rotation")
    dplyr::bind_rows(accel,rotation)
}

#' Entry-point function to parse each filepath of each recordIds
#' walk data and featurize each record using featurize walk data function
#' @params data: dataframe containing filepaths
#' @returns featurized walk data for each participant 
featurize_walk <- function(data, parallel=FALSE){
    features <- plyr::ddply(
        .data = data,
        .variables = all_of(c("recordId", "activityType")),
        .parallel = parallel,
        .fun = function(row){
            tryCatch({ # capture common errors
                ts <- format_v1_time_series(row$accel, row$deviceMotion)
                if(nrow(ts) == 0){
                    stop("ERROR: sensor timeseries is empty")
                }else{
                    featurize_gait(ts)
                }
            }, error = function(err){ # capture all other error
                error_msg <- stringr::str_squish(
                    stringr::str_replace_all(geterrmessage(), "\n", ""))
                return(tibble::tibble(error = error_msg))})}) %>%
        replace(., . =="NaN", NA)
    return(features)
}

#' Entry-point function to parse each filepath of each recordIds
#' walk data and featurize each record using featurize walk data function
#' @params data: dataframe containing filepaths
#' @returns featurized walk data for each participant 
featurize_walk_v2 <- function(data, parallel=FALSE){
    features <- plyr::ddply(
        .data = data,
        .variables = all_of(c("recordId", "fileColumnName")),
        .parallel = parallel,
        .fun = function(row){
            tryCatch({ # capture common errors
                ts <- search_gyro_accel(row$filePath)
                if(nrow(ts) == 0){
                    stop("ERROR: sensor timeseries is empty")
                }else{
                    featurize_gait(ts)
                }
            }, error = function(err){ # capture all other error
                error_msg <- stringr::str_squish(
                    stringr::str_replace_all(geterrmessage(), "\n", ""))
                return(tibble::tibble(error = error_msg))})}) %>%
        replace(., . =="NaN", NA)
    return(features)
}

#' Function to featurize accel/rotation features for 
#' time-series dataset
#' @param data time-series
featurize_gait <- function(data){
    tryCatch({
        feature_list <- list()
        feature_list$acceleration <- data %>%
            dplyr::filter(sensor == "acceleration") %>%
            dplyr::select(t,x,y,z)
        feature_list$rotation <- data %>%
            dplyr::filter(sensor == "rotation") %>%
            dplyr::select(t,x,y,z)
        gait_feature_objs$run_pipeline(feature_list$acceleration, feature_list$rotation)  
    }, error = function(e){
        error_msg <- e$message
        return(tibble::tibble(error = c(error_msg)))
    })
}


main <- function(){
    #' check core usage parameter
    if(is.null(N_CORES)){
        registerDoMC(detectCores())
    }else if(N_CORES > 1){
        registerDoMC(N_CORES)
    }else{
        registerDoMC(1)
    }
    
    walk_ref <- config::get("feature_extraction")$walk
    purrr::map(walk_ref, function(ref){
        window_size <- as.integer(ref$params$window_size)
        gait_feature_objs <<- pdkit_rotation_features$gait_module$GaitFeatures(sensor_window_size = window_size)
        data <- reticulated_get_table(
            syn, 
            tbl_id = SYN_ID_REF$table,
            file_columns = ref$columns,
            query_params = ref$params$query_condition)
        if(MPOWER_VERSION == "v2"){
            features <- data %>%
                dplyr::select(recordId, 
                              fileColumnName, 
                              filePath) %>%
                featurize_walk_v2(parallel = T)
        }else{
            features <- data  %>%
                dplyr::select(recordId, 
                              fileColumnName, 
                              filePath) %>%
                dplyr::mutate(
                    sensorType = ifelse(
                        str_detect(fileColumnName, "^deviceMotion"), 
                        "deviceMotion", "accel"),
                    activityType = case_when(
                        str_detect(fileColumnName, "outbound") ~ "outbound",
                        str_detect(fileColumnName, "return") ~ "return",
                        TRUE ~ "rest")) %>%
                tidyr::pivot_wider(names_from = sensorType, 
                                   values_from = filePath, 
                                   id_cols = all_of(c("recordId", "activityType"))) %>%
                featurize_walk_v1(parallel = T)
        }
        features %>%
            dplyr::rowwise() %>% 
            dplyr::mutate(window = stringr::str_extract(window, '[0-9]+')) %>%
            dplyr::ungroup() %>%
            reticulated_save_to_synapse(
                syn, synapseclient, 
                data = ., 
                output_filename = ref$output_filename,
                parent_id = PARENT_ID,
                annotations = ref$annotations,
                used = WALK_TABLE,
                name = ref$provenance$name,
                description = ref$provenance$description,
                executed = GIT_URL)
    })
}

main()






