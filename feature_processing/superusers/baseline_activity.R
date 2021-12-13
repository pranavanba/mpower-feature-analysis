library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
library(optparse)
source("utils/curation_utils.R")

synapser::synLogin(authToken = "eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC0zODYtMC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTYzOTQzMzg5OCwiaWF0IjoxNjM5NDMzODk4LCJqdGkiOiIxMzU1Iiwic3ViIjoiMzM5NDA0MiJ9.spDsFxZB4jm6Hxyl6XFQUbsCGDJYYx67ELePYxZlewajGRWfPk4jlNrYbaVbh74ZkjG_7tmwZqV6E2bi1CQ9LVD9N8h46Ytwjv0Lzv8l_B6Ase_V29VahHy1AL8nm8_0MvkFVND0PvYQUvjajwVeAtHxK3jYhhrzE57xwN7_27KvYKyZUT-QfW_gPJ-2VdCr-LJpVUeSg7E4qDfMEiTVxiM7PAg2vcX6kCJrC90LtlHQOiDHexK8lcr5f34cxWvbrGBAPzGBVLvhjNfnzObdBIg8R9WUKOM45I5OCi4g1wgHRxItQi7-SVwRHQtxtoDYiWMO6PDfyLCtVgFFulyRgw")

#' Option parser 
option_list <- list(
    make_option(c("-i", "--table_id"), 
                type = "character", 
                default = "syn15673381",
                help = "Synapse ID of mPower Tapping-Activity table entity"),
    make_option(c("-f", "--feature_id"), 
                type = "character", 
                default = "syn26344786",
                help = "Features ID for tapping"),
    make_option(c("-o", "--output_filename"), 
                type = "character", 
                default = "mhealthtools_20secs_tapping_v2_features.tsv",
                help = "Output file name"),
    make_option(c("-p", "--parent_id"), 
                type = "character", 
                default = "syn25691532",
                help = "Output parent ID"),
    make_option(c("-g", "--git_token"), 
                type = "character", 
                default = "~/git_token.txt",
                help = "Path to github token for code provenance"),
    make_option(c("-n", "--provenance_name"), 
                type = "character", 
                default = NULL,
                help = "Provenance parameter for feature extraction"),
    make_option(c("-d", "--provenance_description"), 
                type = "character", 
                default = NULL,
                help = "Provenance description"),
    make_option(c("-m", "--metadata"), 
                type = "character", 
                default = "recordId, createdOn, healthCode, appVersion, phoneInfo, dataGroups, `answers.medicationTiming`",
                help = "Metadata to keep for cleaned data"),
    make_option(c("-a", "--aggregate"), 
                type = "character",
                default = NULL,
                help = "Which index to aggregate on")
)

get_baseline_data <- function(data){
    data %>% 
        dplyr::arrange(createdOn) %>% 
        dplyr::mutate(
            days_since_start = 
                difftime(createdOn, min(.$createdOn), units = "days")) %>% 
        dplyr::filter(days_since_start <= lubridate::ddays(14)) %>%
        dplyr::select(-days_since_start)
}


main <- function(){
    #' get parameter from optparse
    opt_parser = OptionParser(option_list=option_list)
    opt = parse_args(opt_parser)
    
    # Global Variables
    git_url <- get_github_url(
        git_token_path = opt$git_token,
        git_repo = config::get("token_path"),
        script_path = "feature_processing/tapping/clean_tapping_features.R")
    
    # Feature reference
    feature_ref <- list(
        feature_id = opt$feature_id,
        tbl_id = opt$table_id,
        output_parent_id = opt$parent_id,
        output_filename = opt$output_filename,
        name = opt$provenance_name,
        description = opt$provenance_description,
        metadata = opt$metadata)
    
    # get & clean metadata from synapse table
    metadata <- synTableQuery(glue::glue(
        "SELECT {metadata} FROM {table_id} where `substudyMemberships` LIKE '%superusers%'",
        metadata = opt$metadata, 
        table_id = feature_ref$tbl_id))$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>% 
        dplyr::group_by(healthCode) %>% 
        nest() %>% 
        dplyr::mutate(data = purrr::map(data, get_baseline_data)) %>%
        unnest(data) %>% 
        dplyr::ungroup()
    
    
    # merge feature with cleaned metadata
    data <- synGet(feature_ref$feature_id)$path %>% 
        fread() %>%
        dplyr::inner_join(
            metadata, by = c("recordId")) %>%
        dplyr::select(recordId, 
                      createdOn,
                      healthCode, 
                      version, 
                      build,
                      medTimepoint,
                      phoneInfo,
                      everything())
    
    # save to synapse
    save_to_synapse(
        data = data,
        output_filename = feature_ref$output_filename, 
        parent = feature_ref$output_parent_id,
        name = feature_ref$name,
        description = feature_ref$description,
        used = c(feature_ref$tbl_id, feature_ref$feature_id),
        executed = git_url)
}