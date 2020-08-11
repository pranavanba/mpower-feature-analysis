#' This script is used to query walking data
#' using pdkit rotation features using reticulate 
#' virtual environment

library(mhealthtools)
library(reticulate)
library(tidyverse)
library(plyr)
library(jsonlite)
library(doMC)
library(githubr)
registerDoMC(detectCores())

##############################
##### Global Variables #######
##############################
PYTHON_ENV <- "~/env" ## insert python virtualenv here
GIT_PATH <- "~/git_token.txt"
TABLE_SRC <- "syn10308918"
COLUMNS <- c("deviceMotion_walking_outbound.json.items", 
             "deviceMotion_walking_return.json.items")
OUTPUT_FILE <- "pdkit_rotation_walking_features_table_V1.tsv"
PARENT_ID <- "syn22294858"

### set up github credentials
githubr::setGithubToken(readLines(GIT_PATH))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_NAME <- "extractPDKitRotationFeatures_V1_Tables.R"
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = 'R/extractPDKitRotationFeatures_V1_Tables.R')

####################################
#### instantiate python objects #### 
####################################
reticulate::use_virtualenv(PYTHON_ENV, required = TRUE)
gait.feature.py.obj <- reticulate::import("PDKitRotationFeatures")$gait_module$GaitFeatures()
sc <- reticulate::import("synapseclient")
syn <- sc$login()

###########################
#### helper functions ####
###########################
get_pdkit_rotation_features <- function(data){
    #' Function to wrap pdkit rotation features in mHealthtools
    accel.data <- data %>% 
        filter(sensorType == "userAcceleration")
    gyro.data <- data %>% 
        filter(sensorType == "rotationRate")
    features <- gait.feature.py.obj$run_pipeline(accel.data, gyro.data)
    return(features)
}

extract.walk.table <- function(){
    #' Function to query table from synapse, download sensor files
    #' and make it into the valid formatting
    mpower.tbl.entity <- syn$tableQuery(sprintf("SELECT * FROM %s", TABLE_SRC))
    mpower.tbl.data <- mpower.tbl.entity$asDataFrame() %>% 
        dplyr::mutate(
            deviceMotion_walking_outbound.json.items = as.character(
                deviceMotion_walking_outbound.json.items),
            deviceMotion_walking_return.json.items = as.character(
                deviceMotion_walking_return.json.items)) %>%
        dplyr::mutate(medTimepoint = .$medTimepoint%>% unlist())
    mapped.walking.json.files <- syn$downloadTableColumns(mpower.tbl.entity, COLUMNS) %>% 
        data.frame()
    mapped.walking.json.files <- data.frame(
        fileHandleId = names(mapped.walking.json.files) %>% 
            substring(., first = 2) %>% 
            as.character(.),
        jsonPath = as.character(mapped.walking.json.files))
    mpower.tbl.data <- mpower.tbl.data %>% 
        dplyr::left_join(., mapped.walking.json.files, 
                         by = c("deviceMotion_walking_outbound.json.items" = "fileHandleId")) %>%
        dplyr::left_join(., mapped.walking.json.files,
                         by = c("deviceMotion_walking_return.json.items" = "fileHandleId")) %>%
        dplyr::rename(walk.outbound.json=jsonPath.x, 
                      walk.return.json=jsonPath.y)
    return(mpower.tbl.data)
}

shape.time.series.from.json <- function(x){
    #' Function to shape time series from json
    #' and make it into the valid formatting
    ts <- fromJSON(x)
    accel.ts <- ts %>% 
        dplyr::select(matches("userAcceleration|timestamp")) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1],
               sensorType = "userAcceleration") %>%
        flatten() %>%
        dplyr::rename(
            "t" = "timestamp", 
            "x" = "userAcceleration.x", 
            "y" = "userAcceleration.y",
            "z" = "userAcceleration.z")
    rotation.ts <- ts %>% 
        dplyr::select(matches("rotationRate|timestamp")) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1],
               sensorType = "rotationRate") %>% 
        flatten() %>%
        dplyr::rename(
            t = timestamp, 
            x = rotationRate.x, 
            y = rotationRate.y,
            z = rotationRate.z)
    return(rbind(accel.ts, rotation.ts))
}

featurize.table <- function(data, col){
    #' Function to process sensor parallely and bind it into one big feature sets
    walkFeatures <- ddply(.data = data, 
                          .variables=c("recordId", "appVersion","createdOn", 
                                       "healthCode", "phoneInfo", "medTimepoint"), 
                          .parallel=T, 
                          .fun = function(row) { 
                              tryCatch({ 
                                  ts <- shape.time.series.from.json(row[[col]])
                                  features.list <- mhealthtools:::sensor_features(
                                      ts, models = function(x){get_pdkit_rotation_features(x)}) 
                                  return(features.list$model_features[[1]]%>%
                                             mutate(error = as.character(error)))
                              },error = function(err){
                                  return(tibble(error = 'error: NA filepath, unable to process'))})})
    return(walkFeatures %>% 
               dplyr::mutate(error = case_when(error == "NaN" ~ NA)))
}

main <- function(){
    # extract table from synapse
    walk.tbl <- extract.walk.table()
    
    # featurize and bind
    all.walk.features<- dplyr::bind_rows(
        walk.tbl %>% 
            featurize.table(., "walk.outbound.json") %>% 
            mutate(walk_test_direction="outbound"),
        walk.tbl %>% 
            featurize.table(., "walk.return.json") %>%
            mutate(walk_test_direction="return")) %>% 
        write.table(., OUTPUT_FILE, sep="\t", 
                    row.names=F, quote=F)
    
    # save to synapse
    f <- sc$File(OUTPUT_FILE, PARENT_ID)
    f$annotations <- list(features='PDKit and Rotation')
    syn$store(f, activity = sc$Activity(used = c(TABLE_SRC),
                                        executed = GIT_URL))
    unlink(OUTPUT_FILE)
}
main()




    






