###########################################################
#' Script to clean walk features
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
library(optparse)
source("utils/helper_utils.R")
source("utils/curation_utils.R")
synapser::synLogin()

#' login
synapser::synLogin()
CONFIG_PATH <- "templates/config.yaml"
ref <- config::get(file = CONFIG_PATH)

aggregate_walk_features <- function(data, agg_vec){
   data %>%
        dplyr::group_by(
            across(all_of(agg_vec))) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(across(c(all_of(agg_vec), nrecords))) %>%
        dplyr::summarise(across(matches("^x|^y|^z|^AA|^rotation"),
                                list("iqr" = IQR, "md" = median),
                                na.rm = TRUE))
}

separate_rotation_non_rotation <- function(data){
    feature_list <- list()
    # get non rotation segments:
    feature_list$non_rotation <- data %>%
        dplyr::filter(is.na(rotation_omega)) %>%
        dplyr::select(-rotation_omega)
    
    # get rotation segments:
    feature_list$rotation <- data %>%
        dplyr::filter(!is.na(rotation_omega)) %>%
        dplyr::select(healthCode, 
                      recordId, 
                      createdOn,
                      medTimepoint,
                      phoneInfo,
                      rotation_omega)
    return(feature_list)
}


main <- function(){
    # Global Variables
    git_url <- get_github_url(
        git_token_path = ref$git_token_path,
        git_repo = ref$repo_endpoint,
        script_path = "feature_processing/walk30secs/clean_walk30secs_features.R")
    
    # input reference
    input_ref <- list(
        feature_id = synapser::synFindEntityId(
            ref$walk$feature_extraction$output_filename,
            ref$walk$feature_extraction$parent_id),
        table_id = ref$walk$table_id,
        demo_id = synapser::synFindEntityId(
            ref$demo$feature_extraction$output_filename,
            ref$demo$feature_extraction$parent_id),
        git_url = git_url)
    
    # output reference
    output_ref <- list(
        clean = ref$walk$clean,
        agg_users = ref$walk$aggregate_users
    )
    
    # get & clean metadata from synapse table
    metadata <- get_table(input_ref$table_id) %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>%
        curate_app_version() %>%
        curate_med_timepoint() %>%
        curate_phone_info() %>%
        remove_test_user() %>%
        dplyr::select(recordId, 
                      createdOn, 
                      version, 
                      build,
                      phoneInfo,
                      healthCode, 
                      medTimepoint)
    
    # get demo
    demo <- synGet(input_ref$demo_id)$path %>%
        fread(.) %>%
        dplyr::select(healthCode, age, sex, diagnosis)
    
    # merge feature with cleaned metadata
    data <- synGet(input_ref$feature_id)$path %>% 
        fread() %>%
        dplyr::filter(!is.na(window)) %>%
        dplyr::rowwise() %>%
        dplyr::mutate(window = strsplit(window, "_")[[1]][[2]]) %>%
        dplyr::ungroup() %>%
        dplyr::inner_join(
            metadata, by = c("recordId"))  %>%
        dplyr::select(recordId, 
                      createdOn,
                      healthCode, 
                      version, 
                      build,
                      medTimepoint,
                      phoneInfo,
                      everything())
        
        feature_list <- data %>% separate_rotation_non_rotation()
    
        
        # map through aggregated features
        agg_users <- purrr::map(
            feature_list, ~.x %>% 
                aggregate_walk_features(c("healthCode")) %>%
                dplyr::ungroup()) %>%
            purrr::reduce(dplyr::left_join, 
                          by = c("healthCode")) %>%
            dplyr::inner_join(
                demo, by = c("healthCode")) %>%
            dplyr::select(
                healthCode,
                sex, age, diagnosis,
                nrecords = nrecords.x, 
                everything(),
                -nrecords.y)

    # save to synapse (record-level)
    save_to_synapse(
        data = data,
        output_filename = output_ref$clean$output_filename, 
        parent = output_ref$clean$parent_id,
        name = output_ref$clean$provenance$name,
        description = output_ref$clean$provenance$description,
        used = c(input_ref$feature_id, 
                 input_ref$table_id),
        executed = git_url)
    
    # save to synapse (user-level)
    save_to_synapse(
        data = agg_users,
        output_filename = output_ref$agg_users$output_filename, 
        parent = output_ref$agg_users$parent_id,
        name = output_ref$agg_users$provenance$name,
        description = output_ref$agg_users$provenance$description,
        used = c(input_ref$feature_id, 
                 input_ref$table_id),
        executed = git_url)
}

main()