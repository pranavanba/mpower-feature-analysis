library(reticulate)
library(tidyverse)
library(githubr)
library(data.table)
source("R/utils/reticulate_utils.R")

#' import library
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "data_summary","summarize_mpower_activity.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

####################################
#### instantiate global variables #### 
####################################
OUTPUT_REF <- list(
    # walk = list(
    #     output_file = "aggregated_pdkit_rotation_walk_v1_freeze.tsv",
    #     id = "syn25756416",
    #     parent_id = "syn25782484"),
    # tap = list(
    #     output_file = "aggregated_mhealthtools_tap_v1_freeze.tsv",
    #     id = "syn25767029",
    #     parent_id = "syn25782484"),
    # rest = list(
    #     output_file = "aggregated_mpowertools_rest_v1_freeze.tsv",
    #     id = "syn25781982",
    #     parent_id = "syn25782484"),
    tremor_v1 = list(
        agg_hc = "mhealthtools_tremor_freeze_agg_healthcodes.tsv",
        agg_record = "mhealthtools_tremor_freeze_agg_recordId.tsv",
        feat_id = "syn26001251",
        tbl_id = "syn10676309",
        demo_id = "syn25782458",
        parent_id = "syn25782484",
        funs = "summarize_tremor"
    ),
    tremor_v2 = list(
        agg_hc = "mhealthtools_tremor_v2_agg_healthcodes.tsv",
        agg_record = "mhealthtools_tremor_v2_agg_recordId.tsv",
        feat_id = "syn25701650",
        tbl_id = "syn12977322",
        demo_id = "syn25693310",
        parent_id = "syn25999253",
        funs = "summarize_tremor"
    )
)

summarize_users <- function(data){
    data %>%
        dplyr::group_by(healthCode) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::group_by(healthCode, nrecords) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup()
}


summarize_records <- function(data){
    data %>%
        dplyr::group_by(recordId) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup()
}
summarize_walk <- function(){
    data <- syn$get(OUTPUT_REF$walk$id)$path %>% fread() %>%
        dplyr::filter(activityType != "rest") %>% 
        dplyr::select(-starts_with("window"))
    features <- data %>% 
        dplyr::select(matches("^x|^z|^AA_speed|^rotation"), 
                      -matches(
                          "^x_speed_of_gait|^y_speed_of_gait|^z_speed_of_gait")) %>% 
        names(.)
    
    
    walk_data <- data %>% 
        dplyr::filter(is.na(rotation_omega)) %>%
        dplyr::select(all_of(features), -starts_with("rotation_omega")) %>%
        summarize_users() 
    rotation_data <- data %>% 
        dplyr::filter(!is.na(rotation_omega)) %>%
        dplyr::select(healthCode, 
                      recordId, 
                      rotation_omega) %>% 
        summarize_users() %>%
        dplyr::select(healthCode,
                      rotation_omega_md = md,
                      rotation_omega_iqr = iqr)
    walk_data %>% 
        dplyr::left_join(rotation_data, by = c("healthCode"))
}

summarize_rest <- function(){
    data <- syn$get(OUTPUT_REF$rest$id)$path %>% fread()
    data %>% summarize_users()
}


summarize_tap <- function(){
    data <- syn$get(OUTPUT_REF$tap$id)$path %>% 
        fread() %>%
        dplyr::select(-matches("Drift|drift"))
    data %>% summarize_users()
}

summarize_tremor <- function(feature, demo, activity){
    identifier <- syn$tableQuery(
        glue::glue(
            "select recordId, healthCode from {tbl_id}", 
            tbl_id = OUTPUT_REF[[activity]]$tbl_id))$asDataFrame() %>%
        purrr::set_names(
            names(.) %>%
                str_replace("answers.medicationTiming", "medTimepoint")) %>%
        dplyr::select(recordId, healthCode, any_of("medTimepoint"))
    feature <- feature %>%
        dplyr::filter(is.na(error)) %>%
        tidyr::pivot_wider(
            names_from = c(axis, sensor, measurementType),
            names_glue = "{axis}.{sensor}.{measurementType}.{.value}",
            values_from = matches("fr$|tm$")) %>%
        dplyr::select(recordId, activityType, matches("fr$|tm$"))
    agg_record <- feature %>%
        dplyr::group_by(recordId, activityType) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup() %>%
        dplyr::inner_join(identifier, by = c("recordId"))
    agg_hc <- feature %>%
        dplyr::inner_join(identifier, by = c("recordId")) %>%
        dplyr::group_by(healthCode, activityType) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::group_by(healthCode, activityType, nrecords) %>%
        dplyr::summarise_if(is.numeric, 
                            list("md" = median, 
                                 "iqr" = IQR), 
                            na.rm = TRUE) %>%
        dplyr::ungroup()
    return(
        list(
            agg_record = agg_record,
            agg_hc = agg_hc
        )
    )
}


purrr::walk(names(OUTPUT_REF), function(activity){
    feature <- OUTPUT_REF[[activity]]$feat_id %>% 
        syn$get() %>% 
        .$path %>% 
        fread()
    demo <- OUTPUT_REF[[activity]]$demo_id %>% 
        syn$get() %>% 
        .$path %>% 
        fread()
    agg_feat_list <- feature %>%
        summarize_tremor(
            demo = demo, 
            activity = activity)
    
    agg_feat_list$agg_record %>%
        dplyr::inner_join(demo, by = c("healthCode")) %>%
        save_to_synapse(
            syn = syn,
            data = .,
            synapseclient = synapseclient,
            output_filename = OUTPUT_REF[[activity]]$agg_record,
            parent= OUTPUT_REF[[activity]]$parent_id,
            used = OUTPUT_REF[[activity]]$feat_id,
            executed = GIT_URL, 
            name = "aggregate features")
    
    agg_feat_list$agg_hc %>%
        dplyr::inner_join(demo, by = c("healthCode")) %>%
        save_to_synapse(
            syn = syn,
            data = .,
            synapseclient = synapseclient,
            output_filename = OUTPUT_REF[[activity]]$agg_hc,
            parent = OUTPUT_REF[[activity]]$parent_id,
            used = OUTPUT_REF[[activity]]$feat_id,
            executed = GIT_URL, 
            name = "aggregate features")
})
