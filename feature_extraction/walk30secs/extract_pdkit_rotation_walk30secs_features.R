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
source("utils/reticulate_utils.R")
source("utils/curation_utils.R")

#' Get Synapse Creds
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()
syn$table_query_timeout <- 9999999

#' Global Variables of Github Repository and where it is located
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- "feature_extraction/walking/extract_pdkit_rotation_walk30secs_features.R"

#' set global variables for timestamp cutoff
END_TIMESTAMP <- 20.5

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn12514611",
                help = "Synapse ID of mPower Walk-Activity table entity"),
    make_option(c("-f", "--file_column_name"), 
                type = "character", 
                default = "walk_motion.json",
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
                help = "Which mPower Version"))
    
    
parse_accel_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1]) %>%
        dplyr::select(t = timestamp, x, y, z)
}

parse_rotation_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(t = timestamp - .$timestamp[1],
                      x = .$rotationRate$x,
                      y = .$rotationRate$y,
                      z = .$rotationRate$z) %>% 
        dplyr::select(t, x, y, z)
}


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