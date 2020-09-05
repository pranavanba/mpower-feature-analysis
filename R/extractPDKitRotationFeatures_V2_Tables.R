########################################################################
#' Psoriasis Validation
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
registerDoMC(detectCores())

####################################
#### Global Variables ##############
####################################
PYTHON_ENV <- "~/Documents/SageBionetworks/environments/test_venv"
GIT_PATH <- "~/git_token.txt"
WALK_TBL <- "syn12514611"

####################################
#### instantiate python objects #### 
####################################
reticulate::use_virtualenv(PYTHON_ENV, 
                           required = TRUE)
gait.feature.py.obj <- reticulate::import("PDKitRotationFeatures")$gait_module$GaitFeatures()
sc <- reticulate::import("synapseclient")
syn <- sc$login()

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_NAME <- "extractPDKitRotationFeatures_V2_Tables.R"
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = 'R/extractPDKitRotationFeatures_V2_Tables.R')

OUTPUT.PARENT.ID <- "syn22294858"
OUTPUT.FILE <- list()
OUTPUT.FILE$raw_walk_features <- "pdkit_rotation_walking_features_table_V2.tsv"
OUTPUT.FILE$agg_walk_features <- "aggregated_pdkit_rotation_walking_features_table_V2.tsv"


####################################
## Helpers
####################################
#' function to get table, and merge filepaths with filehandleIDs
#' Note: Most recent timepoint will be taken for each participantID
#' @params synID: table entity synapse ID
#' @returns joined table of filepath and filehandleID
get.table <- function(synID, column){
    tbl.entity <- syn$tableQuery(sprintf("SELECT * FROM %s", synID)) 
    mapped.json.files <- syn$downloadTableColumns(tbl.entity, columns = c(column))
    mapped.json.files <- tibble(fileHandleId = names(mapped.json.files),
                                jsonPath = as.character(mapped.json.files))
    joined.df <- tbl.entity$asDataFrame() %>% 
        dplyr::mutate(fileHandleId = as.character(.[[column]])) %>% 
        dplyr::left_join(mapped.json.files, by = c("fileHandleId")) %>%
        dplyr::select("participantID", 
                      "createdOn", 
                      "fileHandleId", 
                      "jsonPath")
    return(joined.df)
}

#' function to shape->featurize walk time series data
#' from digital assessment
#' @params ts: time-series (walk_motion.json from synapse)
#' @returns returns walk features for each record 
featurize.walk.data <- function(ts){
    ts <- ts %>% 
        dplyr::filter(stringr::str_detect(
            sensorType, "userAcceleration|rotationRate")) %>% 
        split(.$sensorType) %>%
        purrr::map(., function(ts){
            ts %>% 
                dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
                dplyr::select(t,x,y,z)})
    features <- gait.feature.py.obj$run_pipeline(ts$userAcceleration, ts$rotationRate)
    return(features)
}

#' Entry-point function to parse each filepath of each recordIds
#' walk data and featurize each record using featurize walk data function
#' @params data: dataframe containing filepaths
#' @returns featurized walk data for each participant 
process.walk.data <- function(data){
    features <- plyr::ddply(
        .data = data,
        .variables = c("participantID", "createdOn", "jsonPath"),
        .parallel = TRUE,
        .fun = function(row){
            tryCatch({
                # print(jsonlite::fromJSON(row$jsonPath))
                ts <- jsonlite::fromJSON(row$jsonPath[[1]])
                if(nrow(ts) == 0){
                    stop()
                }
                return(featurize.walk.data(ts))
            }, error = function(err){
                return(tibble(error = "empty json file"))})})
    return(features)
}

main <- function(){
    #' get raw data
    raw.data <- get.table(WALK_TBL, "walk_motion.json") %>% 
        process.walk.data() %>% 
        dplyr::mutate(createdOn = as.POSIXct(
            createdOn/1000, origin="1970-01-01")) %>% 
        dplyr::mutate(yz_symmetry = y_symmetry * z_symmetry)
    
    #' get aggregated data
    agg.data <- list()
    agg.data$median <- raw.data %>%
        dplyr::group_by(participantID, createdOn) %>%
        dplyr::select(-c("window_end", "window_start", "window_end")) %>%
        dplyr::summarise_if(is.numeric, .funs = c(median), na.rm = TRUE) %>%
        dplyr::rename_if(is.numeric, .funs = function(x) paste0(x, "_med"))
    agg.data$iqr <- raw.data %>%
        dplyr::group_by(participantID, createdOn) %>%
        dplyr::select(-c("window_end", "window_start", "window_end")) %>%
        dplyr::summarise_if(is.numeric, .funs = c(IQR), na.rm = TRUE) %>%
        dplyr::rename_if(is.numeric, .funs = function(x) paste0(x, "_iqr"))
    agg.data <- purrr::reduce(
        agg.data, dplyr::inner_join, 
        by = c("participantID", "createdOn"))
    
    #' map results and store it into synapse
    result.list <- list(raw_walk_features = raw.data,
                        agg_walk_features = agg.data,
                        chosen_agg_walk_features = agg.data %>% dplyr::select(
                            participantID,  createdOn, starts_with("yz")))
    purrr::map(names(result.list), function(data){
        write.to.table <- result.list[[data]] %>% 
            dplyr::rename(participant_id = participantID, date = createdOn) %>%
            write.table(., OUTPUT.FILE[[data]], sep = "\t", row.names=F, quote=F)
        f <- sc$File(OUTPUT.FILE[[data]], OUTPUT.PARENT.ID)
        syn$store(f, activity = sc$Activity(
            "retrieve raw walk features, and aggregated features",
            used = c(WALK_TBL),
            executed = GIT_URL))
        unlink(OUTPUT.FILE[[data]])})
}


main()



