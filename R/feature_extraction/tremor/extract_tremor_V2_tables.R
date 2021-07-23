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
FILE_COLUMNS <- c("right_motion.json", "left_motion.json")
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
NAME <- "extract tremor (time-domain summary) features"
PARALLEL <- TRUE

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_PATH <- 'R/feature_extraction/tremor/extract_tremor_V2_tables.R'
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = SCRIPT_PATH)

OUTPUT_PARENT_ID <- "syn25691532"
OUTPUT_FILE <- "mhealthtools_tremor_features_mpowerV2.tsv"


#' function to detect acceleration and gyroscope information
#' from time series data 
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
process_tremor_samples <- function(filePath){
    tryCatch({ # capture common errors
        ts <- jsonlite::fromJSON(filePath)
        if(nrow(ts) == 0){
            stop("ERROR: sensor timeseries is empty")
        }else{
            ts_list <- ts %>% search_gyro_accel()
            features <- mhealthtools::get_tremor_features(
                accelerometer_data = ts_list$acceleration,
                gyroscope_data = ts_list$rotation,
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

#' function to parallel process tremor features
parallel_process_tremor_features <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            activityType = data$activityType,
                                            filePath = data$filePath), 
                                       function(recordId, activityType, filePath){
                                           process_tremor_samples(filePath) %>% 
                                               dplyr::mutate(
                                                   recordId = recordId,
                                                   activityType = activityType) %>%
                                               dplyr::select(recordId, 
                                                             activityType, 
                                                             everything())})
    data %>% 
        dplyr::select(all_of(c("recordId", "activityType"))) %>%
        dplyr::inner_join(features, by = c("recordId", "activityType"))
}


main <- function(){
    #' get raw data
    tremor_features <- get_table(syn = syn, 
                                 synapse_tbl = TREMOR_TBL,
                                 download_file_columns = FILE_COLUMNS) %>% 
        parse_medTimepoint() %>%
        parse_phoneInfo() %>%
        dplyr::mutate(
            activityType = ifelse(str_detect(fileColumnName, "left"), 
                                  "left_hand_tremor", "right_hand_tremor")) %>%
        parallel_process_tremor_features() %>% 
        save_to_synapse(syn = syn,
                        synapseclient = synapseclient,
                        data = .,
                        output_filename = OUTPUT_FILE,
                        parent = OUTPUT_PARENT_ID,
                        executed = GIT_URL,
                        used = TREMOR_TBL)
}

main()