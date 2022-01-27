###########################################################
#' Script to aggregate features based on user
#' of the walk30secs features
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(synapser)
library(data.table)
library(synapser)
library(tidyverse)
library(githubr)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
source("utils/fetch_id_utils.R")
synapser::synLogin()

#' Global Variables
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$walk,
    feature_extraction = get_feature_extraction_ids(),
    feature_processed = get_feature_processed_ids())
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
SCRIPT_PATH <- file.path(
    "feature_processing", 
    "aggregate_users",
    "aggregate_walk30secs_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)

#' Function to aggregate walk features
#' @data dataframe
#' @agg_vec index to group by
#' @return grouped-user dataframe
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

#' Function to separate rotation features
#' and walk features
#' @param data dataframe
#' @return list of rotation and walk30secs
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

get_metadata <- function(tbl_id){
    # get & clean metadata from synapse table
    metadata <- synTableQuery(glue::glue(
        "SELECT * FROM {tbl_id}"))$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>%
        curate_app_version() %>%
        curate_med_timepoint() %>%
        curate_phone_info() %>%
        remove_test_user()
}


main <- function(){
    refs <- config::get("feature_processing")$walk
    annotations_map <- SYN_ID_REF$feature_extraction %>% 
        get_annotation_mapper()
    metadata <- get_metadata(SYN_ID_REF$table)
    demo <- synGet(SYN_ID_REF$feature_extraction$demo)$path %>%
        fread(.) %>%
        dplyr::select(healthCode, age, sex, diagnosis)
    purrr::map(refs, function(ref){
        if(is.null(ref$annotations$filter)){
            feature_id <- annotations_map %>%
                dplyr::filter(
                    is.na(filter),
                    tool == ref$annotations$tool,
                    analysisType == ref$annotations$analysisType) %>%
                .$id
        }else{
            feature_id <- annotations_map %>%
                dplyr::filter(
                    filter == ref$annotations$filter,
                    tool == ref$annotations$tool,
                    analysisType == ref$annotations$analysisType) %>%
                .$id
        }
        # merge feature with cleaned metadata
        data <- synGet(feature_id)$path %>% 
            fread() %>%
            dplyr::filter(!is.na(window)) %>%
            dplyr::rowwise() %>%
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
        
        # save to synapse
        save_to_synapse(
            data = agg_users,
            output_filename = ref$output_filename, 
            parent = SYN_ID_REF$feature_processed$parent_id,
            annotations = ref$annotations,
            name = ref$provenance$name,
            description = ref$provenance$description,
            used = c(SYN_ID_REF$table, feature_id),
            executed = GIT_URL)
    })
}

main()