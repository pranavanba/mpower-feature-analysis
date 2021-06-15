library(reticulate)
library(tidyverse)
library(githubr)
library(data.table)

#' import library
synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "summarize_mpower_activity.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

####################################
#### instantiate global variables #### 
####################################
WALK_TBL <- "syn25756416"
REST_TBL <- "syn25781982"
TAP_TBL <- "syn25767029"
DEMO_TBL <- "syn25782458"
OUTPUT_REF <- list(
    walk = list(
        output_file = "aggregated_pdkit_rotation_walk_v1_freeze.tsv",
        id = WALK_TBL,
        parent_id = "syn25782484"),
    tap = list(
        output_file = "aggregated_mhealthtools_tap_v1_freeze.tsv",
        id = TAP_TBL,
        parent_id = "syn25782484"),
    rest = list(
        output_file = "aggregated_mpowertools_rest_v1_freeze.tsv",
        id = REST_TBL,
        parent_id = "syn25782484")
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

summarize_walk <- function(){
    data <- syn$get(OUTPUT_REF$walk$id)$path %>% 
        fread() %>%
        dplyr::filter(activityType != "rest") %>% 
        dplyr::select(-starts_with("window"), 
                      -all_of(c("y_speed_of_gait", "x_speed_of_gait", 
                                "z_speed_of_gait", "AA_stride_regularity",
                                "AA_step_regularity", "AA_symmetry")))
    walk_data <- data %>% 
        dplyr::filter(is.na(rotation_omega)) %>%
        dplyr::select(-starts_with("rotation_omega")) %>%
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


demo <- syn$get(DEMO_TBL)$path %>% fread()
purrr::walk(names(OUTPUT_REF), function(activity){
    if(activity == "tap"){
        features <- summarize_tap()
    }else if(activity == "walk"){
        features <- summarize_walk()
    }else{
        features <- summarize_rest()
    }
    
    features %>% 
        dplyr::inner_join(demo, by = c("healthCode")) %>%
        save_to_synapse(
            output_file = OUTPUT_REF[[activity]]$output_file,
            parent_id = OUTPUT_REF[[activity]]$parent_id,
            source_tbl = OUTPUT_REF[[activity]]$id,
            executed = GIT_URL, 
            activity_name = "aggregate features")
})
