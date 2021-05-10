library(tidyverse)
library(jsonlite)
library(doMC)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
source("R/utils.R")

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### Global Variables ##############
####################################
GIT_PATH <- "~/git_token.txt"
TAP_TBL <- "syn15673381"
FILE_COLUMNS <- c("right_tapping.samples", "left_tapping.samples")
UID <- c("recordId")
KEEP_METADATA <- c("healthCode",
                   "createdOn", 
                   "appVersion",
                   "phoneInfo",
                   "operatingSystem",
                   "medTimepoint")

##############################
# Outputs
##############################
setGithubToken(readLines("~/git_token.txt"))
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
SCRIPT_NAME <- "extractTap_V2_Tables.R"
GIT_URL <- githubr::getPermlink(
    "arytontediarjo/feature_extraction_codes", 
    repositoryPath = 'R/extractTap_V2_Tables.R')

OUTPUT_PARENT_ID <- "syn25691532"
OUTPUT_FILE <- "mhealthtools_tapping_features_mpowerV2.tsv"

featurize_tapping <- function(data){
    data <- data %>% 
        tidyr::unnest(location) %>% 
        dplyr::group_by(timestamp) %>% 
        dplyr::mutate(col=seq_along(timestamp)) %>% #add a column indicator
        tidyr::spread(key=col, value=location) %>% 
        dplyr::rename(x = `1`, y = `2`) %>%
        dplyr::mutate(
            buttonid = case_when(
            buttonIdentifier == "left" ~ "TappedButtonLeft",
            buttonIdentifier == "right" ~ "TappedButtonRight",
            TRUE ~ "TappedButtonNone")) %>%
        dplyr::select(t = timestamp, buttonid, x, y) %>% ungroup()
    features <- mhealthtools::get_tapping_features(data) %>% tibble(.)
    return(features)
}

process_tapping_samples <- function(data, parallel = FALSE){
    features <- plyr::ddply(
        .data = data,
        .variables = all_of(c(UID, KEEP_METADATA, "fileColumnName")),
        .parallel = parallel,
        .fun = function(row){
            tryCatch({
                data <- jsonlite::fromJSON(row$filePath)
                if(nrow(data) == 0){
                    stop()
                }
                return(featurize_tapping(data))
            }, error = function(err){
                return(tibble(error = "empty tapping samples"))})}) %>% 
        dplyr::mutate(across(c(UID, KEEP_METADATA), as.character))
    return(features)
}

save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        "extract tap features for mPower V2", 
        executed = GIT_URL,
        used = c(TAP_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}

main <-  function(){
    features <- get_table(syn = syn, synapse_tbl = TAP_TBL,
                          file_columns = FILE_COLUMNS,
                          uid = UID, keep_metadata = KEEP_METADATA) %>%
        parse_medTimepoint() %>%
        parse_phoneInfo() %>%
        process_tapping_samples() %>% 
        save_to_synapse()
}

main()
