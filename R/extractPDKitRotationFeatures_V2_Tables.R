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
                        default="PDkitRotation_walk30s_features_mPowerV2.tsv", 
                        help="synId table source")
    parser$add_argument("-p", 
                        "--parent_id", 
                        type="character", 
                        default="syn24182621", 
                        help="synapse parent id")
    parser$add_argument("-f", 
                        "--filehandle", 
                        type="character", 
                        default="walk_motion.json", 
                        help="synapse parent id")
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
SCRIPT_PATH <- file.path("R", "extractPDKitRotationFeatures_V2_Tables.R")
KEEP_METADATA <- c("recordId","healthCode",
                   "createdOn", "appVersion",
                   "phoneInfo","fileHandleId", 
                   "jsonPath", "medTimepoint")

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

####################################
## Helpers
####################################
#' function to get table, and merge filepaths with filehandleIDs
#' Note: Most recent timepoint will be taken for each participantID
#' @params synID: table entity synapse ID
#' @returns joined table of filepath and filehandleID
get_table <- function(synID, column){
    tbl_entity <- syn$tableQuery(glue::glue("SELECT * FROM {WALK_TBL} LIMIT 10"))
    mapped_json_files <- syn$downloadTableColumns(tbl_entity, columns = c(column))
    mapped_json_files <- tibble(fileHandleId = names(mapped_json_files),
                                jsonPath = as.character(mapped_json_files))
    joined_df <- tbl_entity$asDataFrame() %>% 
        dplyr::mutate(fileHandleId = as.character(.[[column]])) %>% 
        dplyr::left_join(mapped_json_files, by = c("fileHandleId"))
    return(joined_df)
}

clean_android_ts <- function(ts){
    ts <- ts %>% 
        dplyr::filter(stringr::str_detect(
            sensorType, "accel|gyro|rotation")) %>% 
        dplyr::mutate(sensorType = 
                          ifelse(str_detect(sensorType, "accel"), 
                                 "userAcceleration", sensorType),
                      sensorType = 
                          ifelse(str_detect(sensorType, "gyro|rotation"), 
                                 "rotationRate", sensorType)) %>%
        split(.$sensorType) %>%
        purrr::map(., function(ts){
            ts %>% 
                dplyr::select(t = timestamp, x, y, z) %>% 
                drop_na() %>%
                dplyr::mutate_at(
                    .vars = c("x", "y", "z"), 
                    .funs = function(col){col/9.81})})
    return(ts)
}

clean_ios_ts <- function(ts){
    ts <- ts %>% 
        dplyr::filter(stringr::str_detect(
            sensorType, "userAcceleration|rotationRate")) %>% 
        split(.$sensorType) %>%
        purrr::map(., function(ts){
            ts %>% 
                dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
                dplyr::select(t,x,y,z)})
    return(ts)
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
                }else{
                    if(row$operatingSystem != "iOS"){
                        ts <- ts %>% clean_android_ts(.)
                    }else{
                        ts <- ts %>% clean_ios_ts(.)
                    }
                    features <- gait_feature_py_obj$run_pipeline(
                        ts$userAcceleration, ts$rotationRate)
                }
                return(features)
            }, error = function(err){ # capture all other error
                error_msg <- str_squish(str_replace_all(geterrmessage(), "\n", ""))
                return(tibble(error = error_msg))})})
    return(features)
}

main <- function(){
    #' get raw data
    raw_data <- get_table(WALK_TBL, FILEHANDLE) %>%
        dplyr::mutate(
            operatingSystem = ifelse(stringr::str_detect(phoneInfo, "iOS"), "iOS", "Android"))
    if("answers.medicationTiming" %in% names(raw_data)){
        raw_data <- raw_data %>% 
            dplyr::rowwise() %>%
            dplyr::mutate(medTimepoint = glue::glue_collapse(answers.medicationTiming, ", "))
    }else{
        raw_data <- raw_data %>% 
            dplyr::mutate(medTimepoint = NA)
    }
    raw_data <- raw_data  %>%
        tibble::as_tibble(.) %>%
        process_walk_data(.) %>%
        dplyr::mutate(createdOn = as.POSIXct(
            createdOn/1000, origin="1970-01-01")) %>%
        dplyr::select(-fileHandleId, 
                      -jsonPath,
                      everything()) %>% 
        dplyr::mutate(error = na_if(error, "NaN"))
    
    #' store walk30s features
    write.table(raw_data, OUTPUT_FILE, sep = "\t", row.names=F, quote=F)
    f <- sc$File(OUTPUT_FILE, OUTPUT_PARENT_ID)
    syn$store(f, activity = sc$Activity(
        "retrieve raw walk features",
        used = c(WALK_TBL),
        executed = GIT_URL))
    unlink(OUTPUT_FILE)
}

main()



