#############################################
#' This script is used to fetch demographics data
#' and curate based on average age entry per 
#' individual and last entry for metadata
#' like versioning, phoneInfo etc. 
#' 
#' @author aryton.tediarjo@sagebase.org
#############################################
library(tidyverse)
library(githubr)
library(synapser)
library(config)
source("utils/helper_utils.R")
source("utils/fetch_id_utils.R")
synapser::synLogin()


####################################
# Global Vars
####################################
CURRENT_YEAR <- lubridate::year(lubridate::now())
SYN_ID_REF <- list(
    table = config::get("table")$demo,
    feature_extraction = get_feature_extraction_ids())
SCRIPT_PATH <- file.path("feature_extraction", 
                         "demographics",
                         "get_demographics_v2.R")

demo_ref <- config::get("feature_extraction")$demo[[1]]
OUTPUT_REF <- list(
    filename = demo_ref$output_filename,
    parent = SYN_ID_REF$feature_extraction$parent_id,
    annotations = demo_ref$annotations,
    provenance = demo_ref$provenance,
    git_url = get_github_url(
        git_token_path = config::get("git")$token_path,
        git_repo = config::get("git")$repo_endpoint,
        script_path = SCRIPT_PATH)
)

## Factor Level
diagnosis_levels <- c("no_answer", "control", "parkinsons")
sex_levels <- c("no_answer", "male", "female")

#' get demographic info, average age for multiple records
#' get most recent entry for sex, createdOn, diagnosis
#' remove test users
get_demographics_v2 <- function(){
    demo <- synTableQuery(
        glue::glue(
            "SELECT * FROM {demo_tbl}",
            demo_tbl = SYN_ID_REF$table))$asDataFrame() %>% 
        tibble::as_tibble() %>% 
        dplyr::filter(!str_detect(dataGroups, "test")) %>%
        dplyr::mutate(age = CURRENT_YEAR - birthYear,
                      operatingSystem = ifelse(str_detect(phoneInfo, "iOS"), "ios", "android")) %>%
        dplyr::arrange(createdOn) %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
            sex = glue::glue_collapse(sex, sep = ","),
            diagnosis = glue::glue_collapse(diagnosis, sep = ",")) %>%
        dplyr::ungroup() %>%
        dplyr::select(healthCode, 
                      createdOn, age, 
                      sex, diagnosis, 
                      operatingSystem, 
                      phoneInfo) %>%
        dplyr::mutate(
            diagnosis = ifelse(is.na(diagnosis), "no_answer", diagnosis),
            sex = ifelse(is.na(sex), "no_answer", sex)) %>%
        dplyr::mutate(
            diagnosis = factor(
                diagnosis, order = T, levels = diagnosis_levels),
            sex = factor(
                sex, order = T, levels = sex_levels)) %>%
        dplyr::group_by(healthCode) %>%
        dplyr::summarise(age = mean(age, na.rm = TRUE),
                         sex = max(sex),
                         diagnosis = max(diagnosis),
                         phoneInfo = last(phoneInfo),
                         operatingSystem = last(operatingSystem))
    return(demo)
}


main <- function(){
    demo_v2 <- get_demographics_v2() %>% 
        save_to_synapse(output_filename = OUTPUT_REF$filename,
                        parent = OUTPUT_REF$parent, 
                        annotations = OUTPUT_REF$annotations,
                        used = SYN_ID_REF$table,
                        executed = OUTPUT_REF$git_url,
                        name = OUTPUT_REF$provenance$name,
                        description = OUTPUT_REF$provenance$description)
}

main()
