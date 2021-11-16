###########################################################
#' Utility function to extract tapping 
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
library(doMC)
source("utils/curation_utils.R")
source("utils/helper_utils.R")

#' Get Synapse Creds
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()
syn$table_query_timeout <- 9999999
pdkit_rotation_features <- reticulate::import("PDKitRotationFeatures")
gait_feature_objs <- pdkit_rotation_features$gait_module$GaitFeatures(sensor_window_size = 750L)

#' Global Variables of Github Repository and where it is located
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- "feature_extraction/walk30secs/extract_pdkit_rotation_walk30secs_features.R"

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn12514611",
                help = "Synapse ID of mPower Walk-Activity table entity"),
    make_option(c("-f", "--file_column_name"), 
                type = "character", 
                default = "walk_motion.json",
                help = "comma-separated file columns to parse"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "mhealthtools_walk30secs_features_mpowerV2.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn26215077",
                help = "Output parent ID"),
    make_option(c("-c", "--n_cores"), 
                type = "numeric", 
                default = NULL,
                help = "N of cores to use for data processing, null for using all"),
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
                help = "Which mPower Version"))

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
featurize_walk_v1 <- function(data, parallel=FALSE){
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
    #' get parameter from optparse
    opt_parser = OptionParser(option_list=option_list)
    opt = parse_args(opt_parser)
    opt$file_column_name <- stringr::str_replace_all(
        opt$file_column_name, " ", "") %>%
        stringr::str_split(",") %>%
        purrr::reduce(c)
    
    #' get git url
    git_url <- get_github_url(
        git_token_path = opt$git_token, 
        git_repo = GIT_REPO, 
        script_path = SCRIPT_PATH)
    
    
    #' check core usage parameter
    if(is.null(opt$n_cores)){
        registerDoMC(detectCores())
    }else if(opt$n_cores > 1){
        registerDoMC(opt$n_cores)
    }else{
        registerDoMC(1)
    }
    
    #' get table from synapse
    data <- reticulated_get_table(
        syn, 
        tbl_id = opt$table_id,
        file_columns = opt$file_column_name,
        query_params = opt$query_params) 
    
    #' conditional version query for both mpower
    if(opt$mpower_version == 2){
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
    
    #' save to synapse
    features %>%
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






