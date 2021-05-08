library(tidyverse)
library(jsonlite)
library(doMC)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)

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
                   "phoneInfo")

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
OUTPUT_FILE <- "mhealthtools_tap_features_mpowerV2.tsv"

#' function to get table, and merge filepaths with filehandleIDs
#' Note: Most recent timepoint will be taken for each participantID
#' @params synID: table entity synapse ID
#' @returns joined table of filepath and filehandleID
get_tapping_tables <- function(){
    # get table entity
    entity <- syn$tableQuery(glue::glue("SELECT * FROM {TAP_TBL} LIMIT 5"))
    
    # shape table
    table <- entity$asDataFrame() %>%
        tibble::as_tibble(.) %>%
        tidyr::pivot_longer(cols = all_of(FILE_COLUMNS), 
                            names_to = "fileColumnName", 
                            values_to = "fileHandleId") %>%
        dplyr::filter(!is.na(fileHandleId)) %>%
        dplyr::mutate(
            createdOn = as.POSIXct(createdOn/1000, 
                                   origin="1970-01-01"),
            fileHandleId = as.character(fileHandleId))
    
    # download all table columns
    result <- syn$downloadTableColumns(
        table = entity, 
        columns = FILE_COLUMNS) %>%
        tibble::enframe(.) %>%
        tidyr::unnest(value) %>%
        dplyr::select(
            fileHandleId = name, 
            filePath = value) %>%
        dplyr::mutate(filePath = unlist(filePath)) %>%
        dplyr::inner_join(table, by = c("fileHandleId")) %>%
        dplyr::select(all_of(UID), all_of(KEEP_METADATA), 
                      fileColumnName, filePath)
    return(result)
}

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

process_tapping_samples <- function(data){
    features <- plyr::ddply(
        .data = data,
        .variables = all_of(c(UID, KEEP_METADATA, "fileColumnName")),
        .fun = function(row){
            tryCatch({
                data <- jsonlite::fromJSON(row$filePath)
                if(nrow(data) == 0){
                    stop()
                }
                return(featurize_tapping(data))
            }, error = function(err){
                return(tibble(error = "empty tapping samples"))})})
    return(features)
}

main <-  function(){
    features <- get_tapping_tables() %>% 
        process_tapping_samples()  %>%
        readr::write_tsv(., OUTPUT_FILE)
    
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        "extract tap features for mPower V2", 
        executed = GIT_URL,
        used = c(TAP_TBL))
    syn$store(file, activity = activity)
    unlink(OUTPUT.FILE)
    
}

main()


