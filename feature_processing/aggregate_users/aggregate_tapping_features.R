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
source("utils/fetch_id_utils.R")
synapser::synLogin()

#' Global Variables
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$tap,
    feature_extraction = get_feature_extraction_ids(),
    feature_processed = get_feature_processed_ids())
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
SCRIPT_PATH <- file.path(
    "feature_processing", 
    "aggregate_users",
    "aggregate_tapping_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)

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
    refs <- config::get("feature_processing")$tap
    annotations_map <- SYN_ID_REF$feature_extraction %>% 
        get_pipeline_annotation_mapper()
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