library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
library(optparse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")

synapser::synLogin()
CONFIG_PATH <- "templates/config.yaml"

ref <- config::get(file = CONFIG_PATH)
ref_list <- list(ref$tapping, ref$tremor, ref$walk)

filter_enrollment <- function(data){
    data %>% 
        dplyr::arrange(createdOn) %>% 
        dplyr::mutate(
            days_since_start = 
                difftime(createdOn, min(.$createdOn), units = "days")) %>% 
        dplyr::filter(days_since_start <= lubridate::ddays(14)) %>%
        dplyr::select(-days_since_start)
}

get_baseline <- function(table){
    # get & clean metadata from synapse table
    table %>% 
        dplyr::group_by(healthCode) %>% 
        nest() %>% 
        dplyr::mutate(data = purrr::map(data, filter_enrollment)) %>%
        unnest(data) %>% 
        dplyr::ungroup() %>%
        dplyr::select(recordId, 
                      externalId,
                      healthCode, 
                      createdOn,
                      version,
                      build,
                      phoneInfo,
                      medTimepoint)
}


main <- function(){
    # query param
    query_param = "`substudyMemberships` LIKE '%superusers%'"
    
    # Global Variables
    git_url <- get_github_url(
        git_token_path = ref$git_token_path,
        git_repo = ref$repo_endpoint,
        script_path = "feature_processing/superusers/get_baseline_activity.R")
    
    purrr::map(ref_list, function(activity_ref){
        feature_id <- synapser::synFindEntityId(
            activity_ref$feature_extraction$output_filename,
            activity_ref$feature_extraction$parent_id)
        table_id <- activity_ref$table_id
        output_parent_id <- activity_ref$baseline_superusers$parent_id
        output_filename <- activity_ref$baseline_superusers$output_filename
        provenance_name <- "get superusers data"
        provenance_description <- "filter superusers baseline data"
        
        # get metadata to filter sensor
        metadata <- get_table(
            synapse_tbl = table_id, 
            query_params = "where `substudyMemberships` LIKE '%superusers%'") %>%
            curate_med_timepoint() %>%
            curate_app_version() %>%
            curate_phone_info() %>%
            get_baseline()
        
        # merge feature with cleaned metadata
        data <- synGet(feature_id)$path %>% 
            fread() %>%
            dplyr::inner_join(
                metadata, by = c("recordId")) %>%
            dplyr::select(recordId, 
                          externalId,
                          healthCode, 
                          createdOn,
                          version,
                          build,
                          phoneInfo,
                          medTimepoint,
                          everything())
        
        # if window exist, remove NA
        if("window" %in% names(data)){
            data <- data %>%
                tidyr::drop_na(window)
        }
        
        
        # save to synapse
        save_to_synapse(
            data = data,
            output_filename = output_filename, 
            parent = output_parent_id,
            name = provenance_name,
            description = provenance_description,
            used = c(table_id, feature_id),
            executed = git_url)
    })
}

main()