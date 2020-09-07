library(tidyverse)
library(plyr)
library(jsonlite)
library(doMC)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(synapser)

synLogin()
registerDoMC(detectCores())

####################################
#### Global Variables ##############
####################################
GIT_PATH <- "~/git_token.txt"
TAP_TBL <- "syn15673381"
KEEP_METADATA <- c("recordId",
                   "healthCode",
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

OUTPUT.PARENT.ID <- "syn22294858"
OUTPUT.FILE <- "mhealthtools_tap_features_mpowerV2.tsv"

#' function to get table, and merge filepaths with filehandleIDs
#' Note: Most recent timepoint will be taken for each participantID
#' @params synID: table entity synapse ID
#' @returns joined table of filepath and filehandleID
retrieve.tables <- function(synID, filehandles, keepCols = c()){
    tbl.entity <- synTableQuery(sprintf("SELECT * FROM %s LIMIT 50", synID)) 
    mapped.json.files <- synDownloadTableColumns(tbl.entity, 
                                                 columns = filehandles)
    tbl.df <- tbl.entity$asDataFrame()
    get.mapping <- purrr::map(filehandles, function(filehandle){
        mapped.json.files <- dplyr::tibble(
            !!filehandle := names(mapped.json.files),
            "jsonPath" = as.character(mapped.json.files))
        joined.df <- tbl.df %>% 
            dplyr::mutate(fileHandleId = as.character(.[[filehandle]])) %>% 
            dplyr::left_join(mapped.json.files, by = c(filehandle)) %>%
            dplyr::rename(!!paste0(filehandle, "_jsonPath") := jsonPath)}) %>%
        purrr::reduce(dplyr::full_join, by = c("recordId")) %>%
        dplyr::select(recordId, ends_with("_jsonPath"))
    merged.tbl <- tbl.df %>% 
        dplyr::left_join(get.mapping, by = c("recordId")) %>%
        dplyr::select(all_of(keepCols),
                      ends_with("_jsonPath"))
    return(merged.tbl)
}

featurize_tapping <- function(data){
    data <- data %>% unnest(location) %>% 
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

process.tapping.samples <- function(data, col){
    features <- plyr::ddply(
        .data = data,
        .variables = KEEP_METADATA,
        .fun = function(row){
            tryCatch({
                data <- jsonlite::fromJSON(row[[col]])
                if(nrow(data) == 0){
                    stop()
                }
                return(featurize_tapping(data))
            }, error = function(err){
                return(tibble(error = "empty tapping samples"))})})
    return(features)
}

main <-  function(){
    result <- list()
    data <- retrieve.tables(TAP_TBL, 
                            filehandles = c("right_tapping.samples", "left_tapping.samples"),
                            keepCols = KEEP_METADATA)
    
    result$left <- data %>% 
        process.tapping.samples(., "left_tapping.samples_jsonPath") %>% 
        dplyr::rename_with(.cols = -KEEP_METADATA, 
                           .fn = function(x){paste0("left_", x)})
    
    result$right <- data %>% 
        process.tapping.samples(., "right_tapping.samples_jsonPath") %>% 
        dplyr::rename_with(.cols = -KEEP_METADATA, 
                           .fn = function(x){paste0("right_", x)})
    
    result <- result %>% 
        purrr::reduce(dplyr::full_join, 
                      by = KEEP_METADATA) %>%
        write.table(., sep = "\t",row.names=F, quote=F)
    
    file <- synapser::File(
        OUTPUT.FILE, 
        parent=OUTPUT.PARENT.ID)
    activity <- Activity(
        "extract tap features for mPower V2", 
        executed = GIT_URL,
        used = c(TAP_TBL))
    synStore(file, activity = activity)
    unlink(OUTPUT.FILE)
    
}

main()


