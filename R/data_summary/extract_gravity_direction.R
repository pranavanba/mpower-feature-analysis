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
source("R/utils/reticulate_utils.R")

#################################
# Python objects
################################
future::plan(multicore)
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

#################################
# Git reference
################################
SCRIPT_PATH <- file.path("R", "data_summary","extract_gravity_direction.R")
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    GIT_REPO, repositoryPath = SCRIPT_PATH)

#################################
# I/O reference
################################
PARENT_SYN_ID <- "syn25835102"
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo")
OUTPUT_FILE <- "{activity}_phone_orientation_{sensorType}.tsv"

OUTPUT_REF <- list(
    active_v1 = list(
        tbl_id = "syn22241014",
        cols = c("accel_walking_outbound.json.items",
                 "accel_walking_return.json.items",
                 "accel_walking_rest.json.items"),
        output_file_name = OUTPUT_FILE,
        parent = PARENT_SYN_ID,
        name = "get phone orientation using accel - mpower v1"),
    active_v2 = list(
        tbl_id = "syn12514611",
        cols = c("walk_motion.json"),
        output_file_name = OUTPUT_FILE,
        parent = PARENT_SYN_ID,
        name = "get phone orientation using accel - mpower v2"),
    passive = list(
        tbl_id = "syn17022539",
        cols = c("walk_motion.json"),
        output_file_name = OUTPUT_FILE,
        parent = PARENT_SYN_ID,
        name = "extract orientation using accel - passive gait")
)

shape_sensor_v2 <- function(filepath){
    sensor_data <- jsonlite::fromJSON(filepath) 
    if(nrow(sensor_data) == 0){
        stop("ERROR: sensor timeseries is empty")
    }else{
        accel_data <- sensor_data %>% 
            dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
            dplyr::filter(str_detect(tolower(sensorType), "^accel")) %>%
            dplyr::select(t = timestamp, x, y, z)
        return(accel_data)
    }
}

shape_sensor <- function(filePath){
    sensor_data <- jsonlite::fromJSON(filePath) 
    if(nrow(sensor_data) == 0){
        stop("ERROR: sensor timeseries is empty")
    }else{
        if("sensorType" %in% names(sensor_data)){
            sensor_data <- sensor_data %>%
                dplyr::filter(str_detect(tolower(sensorType), "^accel"))
        }else{
            
        }
        accel_data <- sensor_data %>% 
            dplyr::mutate(t = timestamp - .$timestamp[1]) %>%
            dplyr::select(t = timestamp, x, y, z)
        return(accel_data)
    }
}




get_accel_direction <- function(recordId, fileColumnName, filePath) {
    data <- tryCatch({ 
        shape_sensor(filePath) %>% 
                dplyr::summarise(
                    median.x = median(x, na.rm = T),
                    median.y = median(y, na.rm = T),
                    median.z = median(z, na.rm = T)) %>%
                dplyr::mutate(max_acc = max(abs(.), na.rm = T)) %>%
                dplyr::mutate(
                    neg_pos.x = case_when(
                        median.x > 0 ~ "+",
                        TRUE ~ "-"),
                    neg_pos.y = case_when(
                        median.y > 0 ~ "+",
                        TRUE ~ "-"),
                    neg_pos.z = case_when(
                        median.z > 0 ~ "+",
                        TRUE ~ "-"),
                    vertical = case_when(
                        abs(median.x) == max_acc ~ paste0("x", neg_pos.x),
                        abs(median.y) == max_acc ~ paste0("y", neg_pos.y),
                        TRUE ~ paste0("z", neg_pos.z))) %>%
                dplyr::select(starts_with("median"), vertical) %>%
                dplyr::mutate(error = NA_character_)
    }, error = function(e){
        error_msg <-str_squish(geterrmessage()) %>%
            str_replace_all(., "[[:punct:]]", "")
        tibble::tibble(error = error_msg)
    }) 
    data %>% 
       dplyr::mutate(
           recordId = recordId,
           fileColumnName = fileColumnName) %>%
       dplyr::select(
           recordId, 
           fileColumnName, 
           everything())
}


parallel_process_filepath <- function(data, funs){
    features <- furrr::future_pmap_dfr(
        list(recordId = data$recordId, 
             fileColumnName = data$fileColumnName,
             filePath = data$filePath), funs)
    data %>% 
        dplyr::select(all_of(c("recordId", "fileColumnName", KEEP_METADATA))) %>%
        dplyr::inner_join(features, by = c("recordId", "fileColumnName"))
}


main <- function(){
    purrr::map(names(OUTPUT_REF), function(activity){
        accel_direction <- get_table(
            syn = syn, 
            synapse_tbl = OUTPUT_REF[[activity]]$tbl_id,
            file_columns = OUTPUT_REF[[activity]]$cols,
            uid = UID, 
            keep_metadata = KEEP_METADATA) %>% 
            parallel_process_filepath(funs = get_accel_direction) %>%
            save_to_synapse(
                syn = syn,
                synapseclient = synapseclient,
                data = .,
                output_filename = glue::glue(OUTPUT_REF[[activity]]$output_file_name,
                                             activity = activity, 
                                             sensorType = "accel"),
                parent =  OUTPUT_REF[[activity]]$parent,
                name = "get orientation using accel",
                executed = GIT_URL,
                used = OUTPUT_REF[[activity]]$tbl_id
            )
    })
}

main()
    
    

