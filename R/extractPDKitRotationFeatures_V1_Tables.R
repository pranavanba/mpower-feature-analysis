##### query features using new repo #####
library(mhealthtools)
library(reticulate)
library(tidyverse)
library(plyr)
library(jsonlite)
library(doMC)
library(githubr)
registerDoMC(detectCores())


##### Global Variables #### 
PYTHON_ENV <- "~/Documents/SageBionetworks/environments/test_venv" ## insert python virtualenv here
TABLE_SRC <- "syn10308918"
COLUMNS <- c("deviceMotion_walking_outbound.json.items", 
             "deviceMotion_walking_return.json.items")
OUTPUT_FILE <- "pdkit_rotation_walking_features_table_V1.tsv"
PARENT_ID <- "syn22294858"

#### instantiate python env #### 
reticulate::use_virtualenv(PYTHON_ENV, required = TRUE)

#### instantiate python objects #### 
gait.feature.py.obj <- reticulate::import("PDKitRotationFeatures")$gait_module$GaitFeatures()
sc <- reticulate::import("synapseclient")
syn <- sc$login()

#### helper functions ####
get_pdkit_rotation_features <- function(data){
    accel.data <- data %>% filter(sensorType == "userAcceleration")
    gyro.data <- data %>% filter(sensorType == "rotationRate")
    features <- gait.feature.py.obj$run_pipeline(accel.data, gyro.data)
    return(features)
}
extract.walk.table <- function(){
    mpower.tbl.entity <- syn$tableQuery(sprintf("SELECT * FROM %s LIMIT 20", TABLE_SRC))
    mpower.tbl.data <- mpower.tbl.entity$asDataFrame() %>% 
        dplyr::mutate(
            deviceMotion_walking_outbound.json.items = as.character(deviceMotion_walking_outbound.json.items),
            deviceMotion_walking_return.json.items = as.character(deviceMotion_walking_return.json.items))
    mapped.walking.json.files <- syn$downloadTableColumns(mpower.tbl.entity, COLUMNS) %>% data.frame()
    mapped.walking.json.files <- data.frame(
        fileHandleId = names(mapped.walking.json.files) %>% 
            substring(., first = 2),
        jsonPath = as.character(mapped.walking.json.files))
    mpower.tbl.data <- mpower.tbl.data %>% 
        dplyr::left_join(., mapped.walking.json.files, 
                         by = c("deviceMotion_walking_outbound.json.items" = "fileHandleId")) %>%
        dplyr::left_join(., mapped.walking.json.files,
                         by = c("deviceMotion_walking_return.json.items" = "fileHandleId")) %>%
        dplyr::rename(walk.outbound.json=jsonPath.x, walk.return.json=jsonPath.y)
    return(mpower.tbl.data)
}
shape.time.series.from.json <- function(x){
    ts <- fromJSON(x)
    accel.ts <- ts %>% 
        dplyr::select(matches("userAcceleration|timestamp")) %>%
        mutate(timestamp = timestamp - .$timestamp[1],
               sensorType = "userAcceleration") %>% 
        flatten() %>%
        dplyr::rename(
            t = timestamp, 
            x = userAcceleration.x, 
            y = userAcceleration.y,
            z = userAcceleration.z)
    rotation.ts <- ts %>% 
        dplyr::select(matches("rotationRate|timestamp")) %>%
        mutate(timestamp = timestamp - .$timestamp[1],
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
    walkFeatures <- ddply(.data = data, 
                          .variables=c("recordId", "appVersion","createdOn", 
                                       "healthCode", "phoneInfo", "medTimepoint"), 
                          .parallel=T, 
                          .fun = function(row) { 
                              tryCatch({ 
                                  ts <- shape.time.series.from.json(row[[col]])
                                  features.list <- mhealthtools:::sensor_features(
                                      ts, models = function(x){get_pdkit_rotation_features(x)})
                                  return(features.list$model_features[[1]])
                              },error = function(err){
                                  print(paste0('Unable to process ', row[[col]]))
                                  stop(err)})})
    return(walkFeatures)
}

main <- function(){
    walk.tbl <- extract.walk.table()
    all.walk.features<- dplyr::bind_rows(
        walk.tbl %>% 
            featurize.table(., "walk.outbound.json") %>% 
            mutate(testType="outbound"),
        walk.tbl %>% 
            featurize.table(., "walk.return.json") %>%
            mutate(testType="return")) %>% 
        write.table(., OUTPUT_FILE, sep="\t", row.names=F, quote=F)
    
    f <- sc$File(OUTPUT_FILE, PARENT_ID)
    f$annotations <- list(features='PDKit and Rotation')
    syn$store(f, activity = sc$Activity(used = c(TABLE_SRC)))
    unlink(OUTPUT_FILE)
}

main()




    






