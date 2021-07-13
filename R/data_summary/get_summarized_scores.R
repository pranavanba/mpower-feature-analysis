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
SCRIPT_PATH <- file.path("R", "data_summary","get_summarized_scores.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)
OUTPUT_REF <- list(
    active = list(
        output_file = "aggregated_active_walk_score_v2.tsv",
        med_timepoint_file = "aggregated_active_walk_score_medTimepoint_v2.tsv", 
        walk_feature_id = "syn25392705",
        rot_feature_id = "syn25392703"),
    passive = list(
        output_file = "aggregated_passive_walk_score_v2.tsv",
        med_timepoint_file = "aggregated_passive_walk_score_medTimepoint_v2.tsv", 
        walk_feature_id = "syn25392734",
        rot_feature_id = "syn25392732")
)
OUTPUT_PARENT_ID <- "syn25421278"


#' function to summarize features
#' @param walk_data the walk data from segmentation
#' @param rotation_data the rotation data from segmentation
#' @return aggregated data
summarize_features <- function(walk_data, rotation_data){
    features <- walk_data %>% 
        dplyr::select(matches("^x|^z|^AA_speed|^rotation"), 
                      -matches(
                          "^x_speed_of_gait|^y_speed_of_gait|^z_speed_of_gait")) %>% 
        names(.)
    summarize_walk <- walk_data %>% 
        dplyr::filter(window != "window_1", window != "window_2") %>%
        dplyr::group_by(healthCode) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::group_by(healthCode, nrecords) %>%
        dplyr::summarise_at(features, list("md" = median, 
                                           "iqr" = IQR), na.rm = TRUE)
    
    summarize_rotation <- rotation_data %>% 
        dplyr::group_by(healthCode) %>%
        dplyr::summarise_at(c("rotation_omega"), 
                            list("rotation_omega_md" = median, 
                                 "rotation_omega_iqr" = IQR), na.rm = TRUE)
    summarize_walk %>% 
        dplyr::left_join(summarize_rotation, by = c("healthCode"))
}



#' function to summarize features
#' @param walk_data the walk data from segmentation
#' @param rotation_data the rotation data from segmentation
#' @return aggregated data
summarize_features_medTimepoint <- function(walk_data, rotation_data){
    features <- walk_data %>% 
        dplyr::select(matches("^x|^z|^AA_speed|^rotation"), 
                      -matches("^x_speed_of_gait",
                               "^y_speed_of_gait",
                               "^z_speed_of_gait"),
                      -ends_with("var")) %>% 
        names(.)
    summarize_walk <- walk_data %>% 
        tidyr::drop_na(healthCode, medTimepoint) %>%
        dplyr::group_by(healthCode, medTimepoint) %>%
        dplyr::mutate(nrecords = n_distinct(recordId)) %>%
        dplyr::group_by(healthCode, medTimepoint, nrecords) %>%
        dplyr::summarise_at(features, 
                            list("md" = median, 
                                "iqr" = IQR), 
                            na.rm = TRUE)
    
    summarize_rotation <- rotation_data %>% 
        tidyr::drop_na(healthCode, medTimepoint) %>%
        dplyr::group_by(healthCode, medTimepoint) %>%
        dplyr::summarise_at(c("rotation_omega"), 
                            list("rotation_omega_md" = median, 
                                 "rotation_omega_iqr" = IQR), na.rm = TRUE)
    summarize_walk %>% 
        dplyr::left_join(summarize_rotation, 
                         by = c("healthCode", "medTimepoint"))
}

purrr::map(names(OUTPUT_REF), function(walkType){
    walk_id <- OUTPUT_REF[[walkType]]$walk_feature_id
    rot_id <- OUTPUT_REF[[walkType]]$rot_feature_id
    output_name <- OUTPUT_REF[[walkType]]$output_file
    walk_data <- fread(syn$get(walk_id)$path)
    rotation_data <- fread(syn$get(rot_id)$path)
    summarize_features(walk_data, rotation_data) %>% 
        write_tsv(., output_name)
    f <- synapseclient$File(output_name, OUTPUT_PARENT_ID)
    syn$store(
        f, activity = synapseclient$Activity(
            "get aggregated score",
            used = c(walk_id, rot_id),
            executed = GIT_URL))
    unlink(output_name)
})

purrr::map(names(OUTPUT_REF), function(walkType){
    walk_id <- OUTPUT_REF[[walkType]]$walk_feature_id
    rot_id <- OUTPUT_REF[[walkType]]$rot_feature_id
    output_name <- OUTPUT_REF[[walkType]]$med_timepoint_file
    walk_data <- fread(syn$get(walk_id)$path)
    rotation_data <- fread(syn$get(rot_id)$path)
    summarize_features_medTimepoint(
        walk_data, rotation_data) %>% 
        write_tsv(., output_name)
    f <- synapseclient$File(output_name, OUTPUT_PARENT_ID)
    syn$store(
        f, activity = synapseclient$Activity(
            "get aggregated score",
            used = c(walk_id, rot_id),
            executed = GIT_URL))
    unlink(output_name)
})





