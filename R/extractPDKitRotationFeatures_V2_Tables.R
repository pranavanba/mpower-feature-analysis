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
                        default="syn17022539", 
                        help="synId table source")
    parser$add_argument("-o", 
                        "--output", 
                        type="character", 
                        default="PDkitRotation_walk30s_features_mPowerV2.tsv", 
                        help="synId table source")
    parser$add_argument("-p", 
                        "--parent_id", 
                        type="character", 
                        default="syn25381724", 
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

#' global variables
SCRIPT_PATH <- file.path(
    "R", "extractPDKitRotationFeatures_V2_Tables.R")
KEEP_METADATA <- c("recordId",
                   "healthCode",
                   "createdOn",
                   "phoneInfo",
                   "medTimepoint")
REMOVE_FEATURES <- c("y_speed_of_gait", "x_speed_of_gait", 
                     "z_speed_of_gait", "AA_stride_regularity",
                     "AA_step_regularity", "AA_symmetry")
USER_CATEGORIZATION <- "syn17074533"

#' parse arguments
parsed_var <- parse_argument()
WALK_TBL <- parsed_var$tbl_source
PYTHON_ENV <- parsed_var$venv_path
GIT_TOKEN_PATH <- parsed_var$git_token
GIT_REPO <- parsed_var$git_repo
OUTPUT_FILE <- parsed_var$output
OUTPUT_PARENT_ID <- parsed_var$parent_id
FILEHANDLE <- parsed_var$filehandle
WINDOW_SIZE <- as.integer(parsed_var$window_size)

####################################
#### instantiate python objects #### 
####################################
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
#' get user categorization 
get_user_categorization <- function(){
    fread(syn$get(USER_CATEGORIZATION)$path, sep = ",") %>%
        tibble::as_tibble(.) %>% 
        dplyr::filter(userType != "test")
}


#' function to get table, and merge filepaths with filehandleIDs
#' Note: Most recent timepoint will be taken for each participantID
#' @params synID: table entity synapse ID
#' @returns joined table of filepath and filehandleID
get_table <- function(synID, column){
    tbl_entity <- syn$tableQuery(glue::glue("SELECT * FROM {WALK_TBL}"))
    mapped_json_files <- syn$downloadTableColumns(tbl_entity, columns = c(column))
    mapped_json_files <- tibble(fileHandleId = names(mapped_json_files),
                                filePath = as.character(mapped_json_files))
    joined_df <- tbl_entity$asDataFrame() %>% 
        dplyr::mutate(fileHandleId = as.character(.[[column]])) %>% 
        dplyr::left_join(mapped_json_files, by = c("fileHandleId"))
    return(joined_df)
}

shape_sensor_data <- function(ts){
    #' split to list
    ts_list <- list()
    ts_list$acceleration <- ts %>% 
        dplyr::filter(
            stringr::str_detect(tolower(sensorType), "^accel"))
    ts_list$rotation <- ts %>% 
        dplyr::filter(
            stringr::str_detect(tolower(sensorType), "^rotation|^gyro"))
    
    #' parse rotation rate only
    if((stringr::str_detect(
        ts_list$rotation$sensorType, 
        "^gyro|^rotationrate") %>% sum(.)) == 2){
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
process_walk_data <- function(data){
    features <- plyr::ddply(
        .data = data,
        .variables = c("recordId", 
                       "createdOn", 
                       "healthCode", 
                       "phoneInfo",
                       "medTimepoint"),
        .parallel = FALSE,
        .fun = function(row){
            tryCatch({ # capture common errors
                ts <- jsonlite::fromJSON(row$filePath)
                if(nrow(ts) == 0){
                    stop("ERROR: sensor timeseries is empty")
                }else{
                    ts_list <- ts %>% shape_sensor_data(.)
                    gait_feature_py_obj$run_pipeline(
                        ts_list$acceleration, ts_list$rotation)
                }
            }, error = function(err){ # capture all other error
                print(row$recordId)
                error_msg <- stringr::str_squish(
                    stringr::str_replace_all(geterrmessage(), "\n", ""))
                return(tibble::tibble(error = error_msg))})})
    return(features)
}


#' function to check whether medication timepoint exist
check_medTimepoint <- function(data){
    if("answers.medicationTiming" %in% names(data)){
        data %>% 
            dplyr::rowwise() %>%
            dplyr::mutate(medTimepoint = glue::glue_collapse(answers.medicationTiming, ", ")) %>%
            dplyr::ungroup()
    }else{
        data %>% 
            dplyr::mutate(medTimepoint = NA)
    }
}

#' function to parse phone information
parse_phoneInfo <- function(data){
    data %>%
        dplyr::mutate(
            operatingSystem = ifelse(str_detect(phoneInfo, "iOS"), "iOS", "Android"))
}


main <- function(){
    #' get raw data
    gait_features <- get_table(WALK_TBL, FILEHANDLE) %>% 
        check_medTimepoint() %>%
        parse_phoneInfo() %>%
        process_walk_data() %>%
        tibble::as_tibble() %>%
        dplyr::mutate(createdOn = as.POSIXct(
            createdOn/1000, origin="1970-01-01")) %>% 
        dplyr::mutate(error = na_if(error, "NaN")) %>%
        dplyr::inner_join(get_user_categorization(), by = c("healthCode")) %>% 
        segment_gait_data()
    
    #' save all segment to synapse
    purrr::map(names(gait_features), function(segment){
        filename <- glue::glue(segment, "_", parsed_var$output)
        write.table(gait_features[[segment]], 
                    filename, sep = "\t", row.names=F, quote=F)
        f <- sc$File(filename, OUTPUT_PARENT_ID)
        syn$store(f, activity = sc$Activity(
            "retrieve raw walk features",
            used = c(WALK_TBL),
            executed = GIT_URL))
        unlink(filename)
    })
}

main()



