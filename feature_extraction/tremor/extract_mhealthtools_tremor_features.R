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

future::plan(multisession)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

#' Global Variables of Github Repository and where it is located
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- "R/feature_extraction/tremor/extract_mhealthtools_tremor_features.R"

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn12977322",
                help = "Synapse ID of mPower Tremor-Activity table entity"),
    make_option(c("-f", "--file_column_name"), 
                type = "character", 
                default = "right_motion.json,left_motion.json",
                help = "comma-separated file columns to parse"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "mhealthtools_tremor_features_mpower_v2.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn26215076",
                help = "Output parent ID"),
    make_option(c("-c", "--n_cores"), 
                type = "numeric", 
                default = NULL,
                help = "N of cores to use for data processing"),
    make_option(c("-q", "--query_params"), 
                type = "character", 
                default = NULL,
                help = "Additional table query params"),
    make_option(c("-g", "--git_token"), 
                type = "character", 
                default = "~/git_token.txt",
                help = "Path to github token for code provenance"),
    make_option(c("-n", "--provenance_name"), 
                type = "character", 
                default = NULL,
                help = "Provenance parameter for feature extraction"),
    make_option(c("-v", "--mpower_version"), 
                type = "numeric", 
                default = 2,
                help = "Which mPower Version")
)


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
    #' get parameter from optparse
    opt_parser = OptionParser(option_list=option_list)
    opt = parse_args(opt_parser)
    opt$file_column_name <- stringr::str_replace_all(
        opt$file_column_name, " ", "") %>%
        stringr::str_split(",") %>%
        purrr::reduce(c)
    
    #' conditional on mpower version
    if(opt$mpower_version == 1){
        file_parser <- parse_sensor_gyro_accel_v1
    }else{
        file_parser <- parse_sensor_gyro_accel_v2
    }
    
    #' get git url
    git_url <- get_github_url(
        git_token_path = opt$git_token, 
        git_repo = GIT_REPO, 
        script_path = SCRIPT_PATH)
    
    #' check core usage parameter
    if(is.null(opt$n_cores)){
        future::plan(multisession) 
    }else if(opt$n_cores > 1){
        future::plan(strategy = multisession, workers = opt$n_cores) 
    }else{
        future::plan(sequential)
    }
    
    #' - get table from synapse
    #' - download file handle columns
    #' - featurize using mhealthtools
    #' - save to synapse 
    data <- reticulated_get_table(
        syn, tbl_id = opt$table_id,
        file_columns = opt$file_column_name,
        query_params = opt$query_params) %>%
        map_feature_extraction(
            file_parser = parse_sensor_gyro_accel_v2,
            feature_funs = featurize_tremor,
            time_filter = c(5,25), 
            window_length = 256,
            window_overlap = 0.25,
            frequency_filter = c(3, 15),
            detrend = TRUE) %>%
        reticulated_save_to_synapse(
            syn, synapseclient, 
            data = ., 
            output_filename = opt$output_filename,
            parent_id = opt$parent_id, 
            used = opt$table_id,
            name = opt$provenance_name,
            executed = git_url)
}

main()