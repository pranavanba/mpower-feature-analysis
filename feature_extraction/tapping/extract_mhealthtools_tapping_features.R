###########################################################
#' Utility function to extract tapping (parallel or sequentially)
#' features across mPowerV1 and mPowerV2 interchangably
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(mhealthtools)
library(reticulate)
library(furrr)
library(optparse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
source("utils/reticulated_fetch_id_utils.R")

#' Get Synapse Creds
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()
syn$table_query_timeout <- 9999999

#' Global Variables
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$tap,
    feature_extraction = get_feature_extraction_ids(syn = syn))
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
TAP_TABLE <- SYN_ID_REF$table
SCRIPT_PATH <- file.path(
    "feature_extraction", 
    "tapping",
    "extract_mhealthtools_tapping_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)

#' Function to take in raw tappingV1 samples
#' into t,x,y,buttonId format 
#' 
#' @param file_path tapping samples filepath
#' @return dataframe of t,x,y,buttonId 
parse_tapping_v1_samples <- function(file_path){
    data <- fromJSON(file_path)
    if(nrow(data) == 0){
        stop("error: empty files")
    }
    if(is.null(ncol(data))){
        tb <- tibble(t = double(),
                     x = double(),
                     y = double(),
                     buttonid = character())
        return(tb)
    }
    tap_coord <- do.call(
        rbind, map(
            data$TapCoordinate, 
            function(x)stringr::str_extract_all(x, "[[:digit:]]+\\.*[[:digit:]]*")[[1]])) %>% 
        as_tibble() %>% 
        select(x = V1, y = V2) %>% 
        mutate_at(vars(x,y), as.double)
    data <- cbind(data, tap_coord) %>% 
        select(t = TapTimeStamp, x, 
               y, buttonid= "TappedButtonId")
    return(data %>%
               dplyr::arrange(t))
}

#' Function to take in raw tappingV2 samples
#' into t,x,y,buttonId format 
#' 
#' @param file_path tapping samples filepath
#' @return dataframe of t,x,y,buttonId 
parse_tapping_v2_samples <- function(file_path){
    data <- jsonlite::fromJSON(file_path)
    if(nrow(data) == 0){
        stop("error: empty files")
    }
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
        dplyr::select(t = timestamp, buttonid, x, y) %>%
        dplyr::ungroup() %>%
        normalize_timestamp() %>%
        tidyr::drop_na() %>%
        dplyr::arrange(t)
    return(data)
}

#' Featurize tapping samples by mapping
#' each record and file columns using mhealthtools
#' 
#' @param data
#' @return dataframe/tibble tapping features
featurize_tapping <- function(data, ts_cutoff, ...){
    tryCatch({
        data <- data %>%
            dplyr::filter(t < ts_cutoff)
        data %>%
            as.data.frame() %>%
            mhealthtools::get_tapping_features(...) %>% 
            dplyr::mutate(
                end_timestamp = max(
                data$t, na.rm = T)) %>%
            dplyr::relocate(error, .after = last_col())
        
    }, error = function(err){
        error_msg <- stringr::str_squish(
            stringr::str_replace_all(
                geterrmessage(), "\n", ""))
        return(tibble::tibble(error = error_msg))})
}



main <-  function(){
    #' check core usage parameter
    if(is.null(N_CORES)){
        future::plan(multisession) 
    }else if(N_CORES > 1){
        future::plan(strategy = multisession, 
                     workers = N_CORES) 
    }else{
        future::plan(sequential)
    }
    
    
    #' conditional on mpower version
    if(Sys.getenv("R_CONFIG_ACTIVE") == "v1"){
        file_parser <- parse_tapping_v1_samples
    }else{
        file_parser <- parse_tapping_v2_samples
    }
    
    tap_ref <- config::get("feature_extraction")$tap
    purrr::map(tap_ref, function(ref){
        tbl <- reticulated_get_table(
            syn, 
            tbl_id = SYN_ID_REF$table,
            file_columns = ref$columns,
            query_params = ref$params$query_condition)
        features <- tbl %>%
            map_feature_extraction(
                file_parser = file_parser,
                feature_funs = featurize_tapping,
                ts_cutoff = ref$params$ts_cutoff) %>%
            reticulated_save_to_synapse(
                syn, synapseclient, 
                data = ., 
                output_filename = ref$output_filename,
                parent_id = PARENT_ID,
                annotations = ref$annotations,
                used = TAP_TABLE,
                name = ref$provenance$name,
                description = ref$provenance$description,
                executed = GIT_URL)
    })
}

main()
