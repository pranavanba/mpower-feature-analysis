####################################
# get gravity data for passive
####################################
library(reticulate)
library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(argparse)
library(furrr)
source("R/utils.R")

#################################
# Python objects
################################
future::plan(multicore)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

#################################
# Global Variables
################################
WALK_TBL <- "syn12514611"
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
OUTPUT_FILE <- "passive_gait_adherence.tsv"
OUTPUT_PARENT_ID <- "syn24682364"
FILE_COLUMNS <- c("walk_motion.json")
UID <- c("recordId")
SCRIPT_PATH <- file.path("R", "extract_pdkit_rotation_walk_features_v1_table.R")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")
ACTIVITY_NAME <- "extract passive gait gravity direction"

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)

get_gravity_direction <- function(filePath) {
    tryCatch({
        sensor_data <- jsonlite::fromJSON(filePath) 
        if(nrow(sensor_data) == 0){
            stop("ERROR: sensor timeseries is empty")
        }else if(!"gravity" %in% unique(sensor_data$sensorType)){
            stop("ERROR: gravity reading not found")
        }else{
            gravity_data <- sensor_data %>% 
                dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
                dplyr::filter(sensorType == "gravity") %>%
                dplyr::select(t = timestamp, x, y, z)
            mse1 <- mean((gravity_data$x - 1)^2, na.rm = TRUE)
            mse2 <- mean((gravity_data$x + 1)^2, na.rm = TRUE)
            mse3 <- mean((gravity_data$y - 1)^2, na.rm = TRUE)
            mse4 <- mean((gravity_data$y + 1)^2, na.rm = TRUE)
            mse5 <- mean((gravity_data$z - 1)^2, na.rm = TRUE)
            mse6 <- mean((gravity_data$z + 1)^2, na.rm = TRUE)
            aux <- which.min(c(mse1, mse2, mse3, mse4, mse5, mse6))
            if(aux == 1) {
                vertical <- "x+"
            }else if(aux == 3) {
                vertical <- "y+"
            }else if(aux == 5) {
                vertical <- "z+"
            }else if(aux == 2) {
                vertical <- "x-"
            }else if(aux == 4) {
                vertical <- "y-"
            }else {
                vertical <- "z-"
            }
            gravity_data %>% 
                dplyr::summarise(
                    median.x = median(x, na.rm = T),
                    median.y = median(y, na.rm = T),
                    median.z = median(z, na.rm = T)) %>%
                dplyr::mutate(vertical = vertical,
                              error = NA_character_) %>%
                tibble::as_tibble()
        }
    }, error = function(e){
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(as.character(geterrmessage()), 
                                     "[[:punct:]]", ""))
        return(tibble::tibble(error = error_msg))
    })
}


parallel_process_filepath <- function(data){
    features <- furrr::future_pmap_dfr(list(recordId = data$recordId, 
                                            fileColumnName = data$fileColumnName,
                                            filePath = data$filePath), 
                                       function(recordId, fileColumnName, filePath){
                                           get_gravity_direction(filePath) %>% 
                                               dplyr::mutate(
                                                   recordId = recordId,
                                                   fileColumnName = fileColumnName) %>%
                                               dplyr::select(
                                                   recordId, 
                                                   fileColumnName, 
                                                   everything())})
    data %>% 
        dplyr::select(all_of(c("recordId", "fileColumnName", KEEP_METADATA))) %>%
        dplyr::inner_join(features, by = c("recordId", "fileColumnName"))
}

save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        name = ACTIVITY_NAME, 
        executed = GIT_URL,
        used = c(WALK_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}


main <- function(){
    gravity_direction <- get_table(syn = syn, synapse_tbl = WALK_TBL,
                      file_columns = FILE_COLUMNS,
                      uid = UID, keep_metadata = KEEP_METADATA) %>% 
        parse_phoneInfo() %>%
        parse_medTimepoint() %>%
        parallel_process_filepath() %>% 
        save_to_synapse()
}

main()
    
    

