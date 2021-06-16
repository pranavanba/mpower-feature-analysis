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
library(plyr)
library(jsonlite)
library(doMC)
library(githubr)
library(jsonlite)
library(argparse)
library(data.table)
source("R/utils.R")
registerDoMC(detectCores())

####################################
#### Parsing ##############
####################################
#' function to parse argument used for extracting features
parse_argument <- function(){
    parser <- ArgumentParser()
    parser$add_argument("-g", 
                        "--git_token", 
                        type="character", 
                        default="~/git_token.txt", 
                        help="path to github token")
    parser$add_argument("-r", 
                        "--git_repo", 
                        type="character", 
                        default="arytontediarjo/feature_extraction_codes", 
                        help="path to cloned/fork github repo")
    parser$add_argument("-e", 
                        "--venv_path", 
                        type="character", 
                        default="~/env", 
                        help="path to python virtual environment")
    parser$add_argument("-s", 
                        "--tbl_source", 
                        type="character", 
                        default="syn12514611", 
                        help="synId table source")
    parser$add_argument("-o", 
                        "--output", 
                        type="character", 
                        default="PDkitRotation_walk30s_features_mpowerV2.tsv", 
                        help="synId table source")
    parser$add_argument("-p", 
                        "--parent_id", 
                        type="character", 
                        default="syn25691532", 
                        help="synapse parent id")
    parser$add_argument("-f", 
                        "--filehandle", 
                        type="character", 
                        default=c("walk_motion.json", "balance_motion.json"),
                        help="synapse parent id")
    parser$add_argument("-w",
                        "--window_size",
                        type = "integer",
                        default = 750,
                        help = "parameter of gait features")
    return(parser$parse_args())
}

#' global variables
SCRIPT_PATH <- file.path(
    "R", "extractPDKitRotationFeatures_V2_Tables.R")
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
REMOVE_FEATURES <- c("y_speed_of_gait", "x_speed_of_gait", 
                     "z_speed_of_gait", "AA_stride_regularity",
                     "AA_step_regularity", "AA_symmetry")

#' parse arguments
parsed_var <- parse_argument()
WALK_TBL <- parsed_var$tbl_source
PYTHON_ENV <- parsed_var$venv_path
GIT_TOKEN_PATH <- parsed_var$git_token
GIT_REPO <- parsed_var$git_repo
OUTPUT_FILE <- parsed_var$output
OUTPUT_PARENT_ID <- parsed_var$parent_id
FILE_COLUMNS <- parsed_var$filehandle
WINDOW_SIZE <- as.integer(parsed_var$window_size)
PARALLEL <- TRUE

####################################
#### instantiate python objects #### 
####################################
gait_feature_py_obj <- reticulate::import("PDKitRotationFeatures")$gait_module$GaitFeatures(sensor_window_size = WINDOW_SIZE)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)

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
process_walk_samples <- function(data, parallel=FALSE){
    features <- plyr::ddply(
        .data = data,
        .variables = all_of(c(UID, KEEP_METADATA, "fileColumnName")),
        .parallel = parallel,
        .fun = function(row){
            tryCatch({ # capture common errors
                ts <- jsonlite::fromJSON(row$filePath)
                if(nrow(ts) == 0){
                    stop("ERROR: sensor timeseries is empty")
                }else{
                    ts_list <- ts %>% search_gyro_accel()
                    gait_feature_py_obj$run_pipeline(
                        ts_list$acceleration, ts_list$rotation)
                }
            }, error = function(err){ # capture all other error
                print(row$recordId)
                error_msg <- stringr::str_squish(
                    stringr::str_replace_all(geterrmessage(), "\n", ""))
                return(tibble::tibble(error = error_msg))})}) %>%
        dplyr::mutate_at(all_of(KEEP_METADATA), as.character) %>% 
        replace(., . =="NaN", NA)
    return(features)
}

#' function to segment gait data
segment_gait_data <- function(data){
    result <- list()
    data <- data %>% 
        dplyr::filter(is.na(error))
    result$rotation_segment <- data %>% 
        dplyr::filter(!is.na(rotation_omega))
    result$walk_segment <- data %>% 
        dplyr::filter(is.na(rotation_omega)) 
    return(result)
}

save_gait_segments <- function(data){
        purrr::walk(names(data), function(segment){
            filename <- glue::glue(segment, "_", parsed_var$output)
            write_file <- readr::write_tsv(data[[segment]], filename)
            f <- synapseclient$File(filename, OUTPUT_PARENT_ID)
            save_to_synapse <- syn$store(f, activity = synapseclient$Activity(
                "retrieve raw walk features",
                used = c(WALK_TBL),
                executed = GIT_URL))
            unlink(filename)
        })
}

main <- function(){
    #' get raw data
    gait_features <- get_table(syn = syn, synapse_tbl = WALK_TBL,
                               file_columns = FILE_COLUMNS,
                               uid = UID, keep_metadata = KEEP_METADATA) %>% 
        parse_medTimepoint() %>%
        parse_phoneInfo() %>%
        process_walk_samples(parallel = PARALLEL) %>%
        segment_gait_data() %>%
        save_gait_segments()
}

main()



