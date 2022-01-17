###########################################################
#' Script to clean features across mPowerV1 and mPowerV2
#' Optional: Aggregate features based on 
#' which index of the tapping features
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

synapser::synLogin()
CONFIG_PATH <- "templates/config.yaml"
ref <- config::get(file = CONFIG_PATH)

main <- function(){
    # Global Variables
    git_url <- get_github_url(git_token_path = ref$git_token_path,
                              git_repo = ref$repo_endpoint,
                              script_path = "feature_processing/tapping/clean_tapping_features.R")
    # input reference
    input_ref <- list(
        feature_id = synapser::synFindEntityId(
            ref$tapping$feature_extraction$output_filename,
            ref$tapping$feature_extraction$parent_id),
        table_id = ref$tapping$table_id,
        demo_id = synapser::synFindEntityId(
            ref$demo$feature_extraction$output_filename,
            ref$demo$feature_extraction$parent_id),
        git_url = git_url)
    
    
    # output reference
    output_ref <- list(
        clean = ref$tapping$clean,
        agg_users = ref$tapping$aggregate_users
    )
    
    # Feature reference
    feature_ref <- list(
        feature_id = input_ref$feature_id,
        tbl_id = input_ref$table_id,
        demo_id = 'syn26601401',
        output_parent_id = input_ref$parent_id,
        output_filename = input_ref$output_filename,
        git_url = git_url,
        name = input_ref$provenance_name,
        description = input_ref$provenance_description,
        metadata = input_ref$metadata)
    
    # get & clean metadata from synapse table
    metadata <- synTableQuery(glue::glue(
        "SELECT * FROM {table_id}",
        table_id = feature_ref$tbl_id))$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>%
        curate_app_version() %>%
        curate_med_timepoint() %>%
        curate_phone_info() %>%
        remove_test_user()
    
    # get demographics
    demo <- synGet(feature_ref$demo_id)$path %>%
        fread(.) %>%
        dplyr::select(healthCode, age, sex, diagnosis)
    
    # merge feature with cleaned metadata
    data <- synGet(feature_ref$feature_id)$path %>% 
        fread() %>%
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
    
    # aggregate user-level
    agg_users <- data %>%
        dplyr::inner_join(demo, by = c("healthCode")) %>%
        dplyr::group_by(
            across(all_of("healthCode"))) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(across(c(all_of("healthCode"), nrecords))) %>%
        dplyr::summarise(across(matches("Inter|Drift|Taps|XY"),
                                list("iqr" = IQR, "md" = median),
                                na.rm = TRUE)) %>%
        dplyr::ungroup() %>%
        dplyr::inner_join(demo) %>%
        dplyr::select(healthCode, sex, age, 
                      diagnosis, nrecords, everything())
    
    # save to synapse
    save_to_synapse(
        data = data,
        output_filename = output_ref$clean$output_filename, 
        parent = output_ref$clean$parent,
        name = output_ref$clean$provenance$name,
        description = output_ref$clean$provenance$description,
        used = c(input_ref$feature_id, 
                 input_ref$table_id),
        executed = git_url)
    
    # save to synapse
    save_to_synapse(
        data = agg_users,
        output_filename = output_ref$agg_users$output_filename, 
        parent = output_ref$agg_users$parent,
        name = output_ref$agg_users$provenance$name,
        description = output_ref$agg_users$provenance$description,
        used = c(input_ref$feature_id, 
                 input_ref$table_id),
        executed = git_url)
}

main()