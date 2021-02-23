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
                        default="~/Documents/SageBionetworks/environments/test_venv", 
                        help="path to python virtual environment")
    parser$add_argument("-s", 
                        "--tbl_source", 
                        type="character", 
                        default="syn10308918", 
                        help="synId table source")
    parser$add_argument("-o", 
                        "--output", 
                        type="character", 
                        default="PDkitRotation_walk30s_features_mPowerV1.tsv", 
                        help="synId table source")
    parser$add_argument("-p", 
                        "--parent_id", 
                        type="character", 
                        default="syn24182621", 
                        help="synapse parent id")
    parser$add_argument("-f", 
                        "--filehandle", 
                        type="character", 
                        default="deviceMotion_walking_outbound.json.items", 
                        help="synapse file handle for time series")
    parser$add_argument("-w",
                        "--window_size",
                        type = "integer",
                        default = 512,
                        help = "parameter of gait features")
    return(parser$parse_args())
}

parsed_var <- parse_argument()
WALK_TBL <- parsed_var$tbl_source
PYTHON_ENV <- parsed_var$venv_path
GIT_TOKEN_PATH <- parsed_var$git_token
GIT_REPO <- parsed_var$git_repo
OUTPUT_FILE <- parsed_var$output
OUTPUT_PARENT_ID <- parsed_var$parent_id
FILEHANDLE <- parsed_var$filehandle
WINDOW_SIZE <- parsed_var$window_size
SCRIPT_PATH <- file.path("R", "extractPDKitRotationFeatures_V1_Tables.R")
KEEP_METADATA <- c("recordId","healthCode", "createdOn", "appVersion",
                   "phoneInfo","fileHandleId", "jsonPath", "medTimepoint")

####################################
#### instantiate python objects #### 
####################################
reticulate::use_virtualenv(PYTHON_ENV, required = TRUE)
gait_feature_py_obj <- reticulate::import("PDKitRotationFeatures")$gait_module$GaitFeatures(sensor_window_size = WINDOW_SIZE)
sc <- reticulate::import("synapseclient")
syn <- sc$login()

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)


###########################
#### helper functions ####
###########################
featurize_walk_data <- function(ts){
    #' Function to shape time series from json
    #' and make it into the valid formatting
    accel_ts <- ts %>% 
        dplyr::select(matches("userAcceleration|timestamp")) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1],
                      sensorType = "userAcceleration") %>%
        flatten() %>%
        dplyr::rename(
            t = timestamp, 
            x = userAcceleration.x, 
            y = userAcceleration.y,
            z = userAcceleration.z)
    rotation_ts <- ts %>% 
        dplyr::select(matches("rotationRate|timestamp")) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1],
                      sensorType = "rotationRate") %>% 
        flatten() %>%
        dplyr::rename(
            t = timestamp, 
            x = rotationRate.x, 
            y = rotationRate.y,
            z = rotationRate.z)
    features <- gait_feature_py_obj$run_pipeline(accel_ts, rotation_ts)
    return(features)
}

#' Entry-point function to parse each filepath of each recordIds
#' walk data and featurize each record using featurize walk data function
#' @params data: dataframe containing filepaths
#' @returns featurized walk data for each participant 
process_walk_data <- function(data){
    features <- plyr::ddply(
        .data = data,
        .variables = KEEP_METADATA,
        .parallel = TRUE,
        .fun = function(row){
            tryCatch({ # capture common errors
                ts <- jsonlite::fromJSON(row$jsonPath)
                if(nrow(ts) == 0){
                    stop("ERROR: sensor timeseries is empty")
                }else if(!all(c("userAcceleration", "rotationRate") %in% 
                              (names(ts)))){
                    stop("ERROR: user accel and rotation rate not available")
                }else{
                    return(featurize_walk_data(ts))
                }
            }, error = function(err){ # capture all other error
                error_msg <- str_squish(str_replace_all(geterrmessage(), "\n", ""))
                return(tibble(error = error_msg))})})
    return(features)
}


get_table <- function(WALK_TBL, FILEHANDLE){
    #' Function to query table from synapse, download sensor files
    #' and make it into the valid formatting
    mpower_tbl_entity <- syn$tableQuery(sprintf("SELECT * FROM %s", WALK_TBL))
    mpower_tbl_data <- mpower_tbl_entity$asDataFrame() %>% 
        tibble::as_tibble(.) %>%
        dplyr::mutate(
            !!sym(FILEHANDLE) := as.character(
                !!sym(FILEHANDLE)),
            medTimepoint = .$medTimepoint%>% unlist()) %>%
        dplyr::select(everything(), fileHandleId := !!sym(FILEHANDLE))
    mapped_walking_json_files <- syn$downloadTableColumns(
        mpower_tbl_entity, 
        FILEHANDLE) %>% 
        data.frame()
    mapped_walking_json_files <- data.frame(
        fileHandleId = names(mapped_walking_json_files) %>% 
            substring(., first = 2) %>% 
            as.character(.),
        jsonPath = as.character(mapped_walking_json_files))
    mpower_tbl_data <- mpower_tbl_data %>% 
        dplyr::left_join(., mapped_walking_json_files)
    return(mpower_tbl_data)
}


main <- function(){
    #' get raw data
    raw_data <- get_table(WALK_TBL, FILEHANDLE) %>% 
        process_walk_data() %>%
        dplyr::mutate(createdOn = as.POSIXct(
            createdOn/1000, origin="1970-01-01")) %>%
        dplyr::select(-fileHandleId, -jsonPath) %>% 
        dplyr::mutate(error = na_if(error, "NaN"))
    
    #' store walk30s features
    write.table(raw_data, OUTPUT_FILE, sep = "\t", row.names=F, quote=F)
    f <- sc$File(OUTPUT_FILE, OUTPUT_PARENT_ID)
    syn$store(f, activity = sc$Activity(
        "retrieve v1 raw walk features",
        used = c(WALK_TBL),
        executed = GIT_URL))
    unlink(OUTPUT_FILE)
}

main()

