###########################################################
#' Script to clean tremor features
#' based on 30 best features from genetic algo
#' Requires: reticulated python (parallel group by)
#' 
#' @author: aryton.tediarjo@sagebase.org
############################################################
library(tidyverse)
library(githubr)
library(data.table)
library(furrr)
library(future)
source("utils/helper_utils.R")
source("utils/fetch_id_utils.R")

#' login to synapse using reticulate
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login()
syn$table_query_timeout <- 9999999

#' Global Variables
N_CORES <- config::get("cpu")$n_cores
SYN_ID_REF <- list(
    table = config::get("table")$tremor,
    feature_extraction = get_feature_extraction_ids(syn),
    feature_processed = get_feature_processed_ids(syn))
PARENT_ID <- SYN_ID_REF$feature_extraction$parent_id
SCRIPT_PATH <- file.path(
    "feature_processing", 
    "aggregate_users",
    "aggregate_tremor_features.R")
GIT_URL = get_github_url(
    git_token_path = config::get("git")$token_path,
    git_repo = config::get("git")$repo_endpoint,
    script_path = SCRIPT_PATH)
BEST_FEATURES_ID <- "syn17088603"

####################################
# instantiate named list variable #
####################################

# map kinetic measurement type to abbreviations
KINETIC_MAPPING <- list(
    acceleration = c(
        "acceleration" = "ua",
        "jerk" = "uj",
        "displacement" = "ud",
        "velocity" = "uv",
        "acf" = "uaacf"),
    gyroscope = c(
        "acceleration" = "uaa",
        "jerk" = "uaj",
        "displacement" = "uad",
        "velocity" = "uav",
        "acf" = "uavcf"
    ))

#' function to get top n features
top_n_features <- function(syn, best_features_id, n = 30){
    syn$get(best_features_id)$path %>% 
        fread() %>%
        dplyr::filter(assay == "rest") %>%
        dplyr::slice(1:n) %>% 
        .$Feature %>%
        glue::glue_collapse(sep = "|")
}

#' function to get table and parse 
#' medication columns accordingly
get_table <- function(syn, tbl_id){
    syn$tableQuery(
        glue::glue("SELECT * FROM {id}", id = tbl_id))$asDataFrame() %>%
        dplyr::rename_with(
            .cols = matches("answers.medicationTiming"), 
            .fn = ~"medTimepoint") %>%
        dplyr::mutate(
            medTimepoint = unlist(medTimepoint), 
            createdOn = as.character(lubridate::as_datetime(createdOn/1000))) %>%
        dplyr::select(recordId,
                      createdOn,
                      phoneInfo,
                      appVersion,
                      healthCode, 
                      medTimepoint) %>%
        tibble::as_tibble()
}

#' function to map kinetic features based on 
#' mpower tools to mhealthtools
#' 
#' @param feature the feature sets
#' 
#' @return a mapped kinetic features
map_kinetic_features <- function(feature){
    print("mapping kinetic features")
    feature %>%
        dplyr::mutate(
            measurementType = case_when(
                str_detect(sensor, "accelerometer") ~ str_replace_all(
                    measurementType, KINETIC_MAPPING$acceleration), 
                str_detect(sensor, "gyroscope") ~ str_replace_all(
                    measurementType, KINETIC_MAPPING$gyroscope), 
                TRUE ~ measurementType))
}

summarise_features <- function(feature){
    feature %>%
        dplyr::summarise(across(
            .cols = everything(),
            .fns = list(md = ~median(.x, na.rm = TRUE), 
                        iqr = ~IQR(.x, na.rm = TRUE)),
            .names = "{.col}.{.fn}"))
}

#' function to aggregate features
#' for the tremor data
#' 
#' @param feature the feature sets
#' @param group the grouping for the aggregates
#' 
#' @param return an aggregated features of .fr, and .tm
group_features <- function(feature, group) {
    feature %>%
        dplyr::mutate(
            across(matches(".fr|.tm"), as.numeric)) %>%
        dplyr::group_by(across(all_of(c(group, 
                                        "fileColumnName", 
                                        "sensor", 
                                        "measurementType", 
                                        "axis")))) %>%
        dplyr::select(matches(".fr|.tm")) %>%
        tidyr::nest() %>%
        dplyr::bind_cols(future_map_dfr(.$data, summarise_features)) %>%
        dplyr::select(-data) %>%
        dplyr::ungroup()
}


#' function to melt feature of the tremor data
#' 
#' @param feature the feature sets
#' 
#' @return a long-to-wide dataframe of feature.agg_measurementType_sensor_axis
widen_features <- function(feature){
    feature %>%
        tidyr::pivot_wider(
            names_from = c(sensor, measurementType, axis),
            names_glue = "{.value}_{measurementType}_{sensor}_{axis}",
            values_from = matches(".md$|.iqr$"))
}

get_metadata <- function(tbl_id, syn){
    # get & clean metadata from synapse table
    metadata <- syn$tableQuery(glue::glue(
        "SELECT * FROM {tbl_id}"))$asDataFrame() %>%
        curate_app_version() %>%
        curate_med_timepoint() %>%
        curate_phone_info() %>%
        remove_test_user()
}

main <- function(){
    refs <- config::get("feature_processing")$tremor
    annotations_map <- SYN_ID_REF$feature_extraction %>% 
        reticulated_get_annotation_mapper()
    metadata <- get_metadata(SYN_ID_REF$table, syn = syn)
    demo <- syn$get(SYN_ID_REF$feature_extraction$demo)$path %>%
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
        # get features aggregated with recordId
        syn$get(feature_id)$path %>%
            fread() %>%
            dplyr::inner_join(metadata, by = c("recordId")) %>%
            map_kinetic_features()  %>%
            group_features(group = c("healthCode")) %>%
            widen_features() %>%
            dplyr::inner_join(demo, by = c("healthCode")) %>% 
            dplyr::select(healthCode, 
                          diagnosis, 
                          age,
                          sex, 
                          matches(
                              top_n_features(
                                  syn = syn,
                                  best_features_id = BEST_FEATURES_ID))) %>%
            # save data via reticulated synapse
            reticulated_save_to_synapse(
                data = .,
                syn = syn, 
                synapseclient = synapseclient,
                output_filename = ref$output_filename, 
                parent = SYN_ID_REF$feature_processed$parent_id,
                annotations = ref$annotations,
                name = ref$provenance$name,
                description = ref$provenance$description,
                used = c(SYN_ID_REF$table, feature_id, BEST_FEATURES_ID),
                executed = GIT_URL
            )
    })
        
}

main()
    