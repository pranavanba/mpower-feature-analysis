###########################################################
#' Utility function to extract tremor
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
library(furrr)
library(optparse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
source("utils/reticulated_fetch_id_utils.R")

future::plan(multisession)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

#' Global Variables
MPOWER_VERSION <- Sys.getenv("R_CONFIG_ACTIVE")
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$tremor,
    feature_extraction = get_feature_extraction_ids(syn = syn))
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
TREMOR_TABLE <- SYN_ID_REF$table
SCRIPT_PATH <- file.path(
    "feature_extraction", 
    "tremor",
    "extract_mhealthtools_tremor_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)


#' Function to detect acceleration and 
#' gyroscope information from file path
#' 
#' @param file_path .json motion file from Synapse table
#' @return a tibble/dataframe of sensor 
parse_sensor_gyro_accel_v1 <- function(file_path){
    ts <- jsonlite::fromJSON(file_path)
    if(nrow(ts) == 0){
        stop("ERROR: sensor timeseries is empty")
    }else{
        #' split to list
        ts_list <- list()
        ts_list$acceleration <- ts %>%
            dplyr::mutate(t = timestamp - .$timestamp[1],
                          x = .$userAcceleration$x,
                          y = .$userAcceleration$y,
                          z = .$userAcceleration$z) %>%
            dplyr::select(t, x, y, z) %>%
            dplyr::mutate(sensorType = "accelerometer")
        ts_list$rotation <- ts %>%
            dplyr::mutate(t = timestamp - .$timestamp[1],
                          x = .$rotationRate$x,
                          y = .$rotationRate$y,
                          z = .$rotationRate$z) %>% 
            dplyr::select(t, x, y, z) %>%
            dplyr::mutate(sensorType = "gyro")
        
        #' parse rotation rate only
        if(length(ts_list$rotation$sensorType %>% unique(.)) > 1){
            ts_list$rotation <- ts_list$rotation %>% 
                dplyr::filter(!stringr::str_detect(tolower(sensorType), "^gyro"))
        }
        
        ts <- ts_list %>%
            purrr::map(., ~(.x %>%
                                dplyr::select(sensorType, t,x,y,z))) %>%
            purrr::reduce(dplyr::bind_rows)
    }
    return(ts)
}


#' Function to detect acceleration and 
#' gyroscope information from file path
#' 
#' @param file_path .json motion file from Synapse table
#' @return a tibble/dataframe of sensor 
parse_sensor_gyro_accel_v2 <- function(file_path){
    ts <- jsonlite::fromJSON(file_path)
    if(nrow(ts) == 0){
        stop("ERROR: sensor timeseries is empty")
    }else{
        #' split to list
        ts_list <- list()
        ts_list$acceleration <- ts %>% 
            dplyr::filter(
                stringr::str_detect(tolower(sensorType), "^accel")) %>%
            dplyr::mutate(sensorType = "accelerometer")
        ts_list$rotation <- ts %>% 
            dplyr::filter(
                stringr::str_detect(tolower(sensorType), "^rotation|^gyro")) %>%
            dplyr::mutate(sensorType = "gyro")
        
        #' parse rotation rate only
        if(length(ts_list$rotation$sensorType %>% unique(.)) > 1){
            ts_list$rotation <- ts_list$rotation %>% 
                dplyr::filter(!stringr::str_detect(tolower(sensorType), "^gyro"))
        }
        
        ts <- ts_list %>%
            purrr::map(., ~(.x %>%
                                dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
                                dplyr::select(sensorType, t,x,y,z))) %>%
            purrr::reduce(dplyr::bind_rows)
    }
    return(ts)
}



#' Featurize tapping samples by mapping
#' each record and file columns using mhealthtools
#' 
#' @param data
#' @return dataframe/tibble tapping features
featurize_tremor <- function(data, ...){
    tryCatch({
        accel <- data %>%
            dplyr::filter(sensorType == "accelerometer") %>%
            dplyr::select(t,x,y,z) %>%
            normalize_timestamp() %>%
            tidyr::drop_na() %>%
            as.data.frame()
        gyro <- data %>%
            dplyr::filter(sensorType == "gyro")  %>%
            dplyr::select(t,x,y,z) %>%
            normalize_timestamp() %>%
            tidyr::drop_na() %>%
            as.data.frame()
        features <- mhealthtools::get_tremor_features(
            accelerometer_data = accel,
            gyroscope_data = gyro,
            derived_kinematics = TRUE,
            funs = c(mhealthtools::time_domain_summary,
                     mhealthtools::frequency_domain_summary),
            ...)
        if (is.null(features$error) && !is.null(features$extracted_features)) {
            return(features$extracted_features %>%
                       dplyr::ungroup() %>%
                       dplyr::mutate(error = NA))
        } else {
            return(tibble::tibble(
                error = as.character(stringr::str_c(features$error)) %>%
                    str_replace_all("[[:punct:]]", "")))
        }
    }, error = function(err){
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(
                geterrmessage(), "\n", ""))
        return(tibble::tibble(error = error_msg))})
}


main <- function(){
    #' check core usage parameter
    if(is.null(N_CORES)){
        future::plan(multisession) 
    }else if(N_CORES > 1){
        future::plan(strategy = multisession, 
                     workers = N_CORES) 
    }else{
        future::plan(sequential)
    }
    
    #' conditional on mpower version
    if(MPOWER_VERSION == "v1"){
        file_parser <- parse_sensor_gyro_accel_v1
    }else{
        file_parser <- parse_sensor_gyro_accel_v2
    }
    
    tremor_ref <- config::get("feature_extraction")$tremor
    purrr::map(tremor_ref, function(ref){
        tbl <- reticulated_get_table(
            syn, 
            tbl_id = SYN_ID_REF$table,
            file_columns = ref$columns,
            query_params = ref$params$query_condition)
        features <- tbl %>%
            map_feature_extraction(
                file_parser = file_parser,
                feature_funs = featurize_tapping,
                ts_cutoff = ref$params$ts_cutoff) %>%
            reticulated_save_to_synapse(
                syn, synapseclient, 
                data = ., 
                output_filename = ref$output_filename,
                parent_id = PARENT_ID,
                annotations = ref$annotations,
                used = TREMOR_TABLE,
                name = ref$provenance$name,
                description = ref$provenance$description,
                executed = GIT_URL)
    })
}

main()