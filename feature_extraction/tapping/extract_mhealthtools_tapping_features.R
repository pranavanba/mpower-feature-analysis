###########################################################
#' Utility function to extract tapping 
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

#' Get Synapse Creds
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()
syn$table_query_timeout <- 9999999

#' Global Variables of Github Repository and where it is located
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- "feature_extraction/tapping/extract_mhealthtools_tapping_features.R"

#' set global variables for timestamp cutoff
END_TIMESTAMP <- 20.5

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn15673381",
                help = "Synapse ID of mPower Tapping-Activity table entity"),
    make_option(c("-f", "--file_column_name"), 
                type = "character", 
                default = "right_tapping.samples,left_tapping.samples",
                help = "comma-separated file columns to parse"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "mhealthtools_tapping_features_mpowerV2.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn25691532",
                help = "Output parent ID"),
    make_option(c("-c", "--n_cores"), 
                type = "numeric", 
                default = NULL,
                help = "N of cores to use for data processing"),
    make_option(c("-q", "--query_params"), 
                type = "character", 
                default = NULL,
                help = "Additional table query params"),
    make_option(c("-g", "--git_token"), 
                type = "character", 
                default = "~/git_token.txt",
                help = "Path to github token for code provenance"),
    make_option(c("-n", "--provenance_name"), 
                type = "character", 
                default = NULL,
                help = "Provenance parameter for feature extraction"),
    make_option(c("-v", "--mpower_version"), 
                type = "numeric", 
                default = 2,
                help = "Which mPower Version")
)

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
featurize_tapping <- function(data, ...){
    tryCatch({
        data <- data %>%
            dplyr::filter(t < 20.5)
        
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
    #' get parameter from optparse
    opt_parser = OptionParser(option_list=option_list)
    opt = parse_args(opt_parser)
    opt$file_column_name <- stringr::str_replace_all(
        opt$file_column_name, " ", "") %>%
        stringr::str_split(",") %>%
        purrr::reduce(c)
    
    #' conditional on mpower version
    if(opt$mpower_version == 1){
        file_parser <- parse_tapping_v1_samples
    }else{
        file_parser <- parse_tapping_v2_samples
    }
    
    #' get git url
    git_url <- get_github_url(
        git_token_path = opt$git_token, 
        git_repo = GIT_REPO, 
        script_path = SCRIPT_PATH)
    
    #' check core usage parameter
    if(is.null(opt$n_cores)){
       future::plan(multisession) 
    }else if(opt$n_cores > 1){
        future::plan(strategy = multisession, 
                     workers = opt$n_cores) 
    }else{
        future::plan(sequential)
    }
    
    #' - get table from synapse
    #' - download file handle columns
    #' - featurize using mhealthtools
    #' - save to synapse 
    data <- reticulated_get_table(
        syn, tbl_id = opt$table_id,
        file_columns = opt$file_column_name,
        query_params = opt$query_params) %>%
        map_feature_extraction(
            file_parser = file_parser,
            feature_funs = featurize_tapping) %>%
        reticulated_save_to_synapse(
            syn, synapseclient, 
            data = ., 
            output_filename = opt$output_filename,
            parent_id = opt$parent_id, 
            used = opt$table_id,
            name = opt$provenance_name,
            executed = git_url)
}

main()
