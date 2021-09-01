library(synapser)
library(tidyverse)
library(data.table)

synapser::synLogin()

PARENT_ID <- "syn26142249"
FEATURE_SYN_ID_LIST <- list(
    tapping = "syn25692238",
    walking = "syn25692284",
    rotation = "syn25692281",
    tremor = "syn26042630"
)

OUTPUT_FILENAME <- list(
    tapping = "udall_tapping_features.tsv",
    walking = "udall_walk30s_features.tsv",
    tremor = "udall_tremor_features.tsv"
)


FEATURE_SELECTION <- list(
    tapping = c('meanTapInter', 
                'medianTapInter', 
                'iqrTapInter',
                'numberTaps'),
    walking = c("x_speed_of_gait", 
             "x_cadence", 
             "x_step_duration",
             "x_stride_duration",
             "x_step_deviation",
             "x_stride_deviation"),
    rotation = c("rotation_omega"),
    tremor = c("cent.fr.iqr_ud_accelerometer",
                "Q75.fr.iqr_uj_accelerometer",
                "mean.tm.iqr_uavacf_gyroscope")
)

externalIds_mapping <- tibble::tibble(
    healthCode = c('e069767a-81bf-44da-9fa4-3b22c2c54776',
                   'eabfe044-9456-47c5-9cc9-eca61c54fd29',
                   '96ba9749-d925-4470-8bff-42ab2abafa25',
                   '53f012ae-c4ac-4090-ab12-1f0baa021ac6'),
    externalId = c('NIHKH638RXUVN',
                   'NIHXN551LBFMK',
                   'NIHHG558EJJMM',
                   'NIHAV871KZCVE'),
    diagnosis = c(
        "control",
        "PD",
        "control",
        "PD"),
    age = c(71, 72, 58, 63))


GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "data_summary","udall_feature_extraction.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

#' filter data by 20 days from initial createdOn
filter_by_first_study_burst <- function(data){
    data %>%
        dplyr::filter(createdOn <= min(createdOn) + lubridate::ddays(20))
}

#' helper function to filter study burst per user
filter_data <- function(data){
    data %>%
        dplyr::inner_join(externalIds_mapping, by = "healthCode") %>%
        dplyr::group_by(healthCode) %>%
        tidyr::nest() %>%
        dplyr::mutate(data = purrr::map(data, filter_by_first_study_burst)) %>%
        tidyr::unnest(data) %>%
        dplyr::ungroup()
}


#' Function to extract tremor features
#' and top 5 most important feature 
#' from previous analysis
get_walk_features <- function(walk_segment_id, rotation_segment_id){
    walk_segment <- synapser::synGet(walk_segment_id)$path %>% 
        data.table::fread() %>%
        tibble::as_tibble() %>%
        dplyr::group_by(recordId, healthCode, createdOn, medTimepoint) %>%
        dplyr::select(all_of(FEATURE_SELECTION$walking)) %>%
        dplyr::summarise_all(list("window_md" = median, 
                                  "window_iqr" = IQR), na.rm = TRUE)
    rot_segment <- synapser::synGet(rotation_segment_id)$path %>% 
        data.table::fread() %>%
        tibble::as_tibble() %>%
        dplyr::group_by(recordId) %>%
        dplyr::select(all_of(FEATURE_SELECTION$rotation)) %>%
        dplyr::summarise_all(list("rotation_omega_md" = median, 
                                  "rotation_omega_iqr" = IQR), na.rm = TRUE)
    
    walk_segment %>%
        dplyr::left_join(
            rot_segment, 
            by = c("recordId")) %>%
        dplyr::ungroup()
}

#' Function to extract tremor features
#' and top 4 most important feature 
#' from previous analysis
get_tap_features <- function(tap_id){
    synapser::synGet(tap_id)$path %>% 
        data.table::fread() %>%
        tibble::as_tibble() %>%
        dplyr::mutate(activityType = ifelse(
            stringr::str_detect(fileColumnName, 
                                "left"), 
            "left_hand_tapping", 
            "right_hand_tapping")) %>%
        dplyr::select(recordId, 
                      healthCode, 
                      createdOn, 
                      medTimepoint,
                      activityType,
                      all_of(FEATURE_SELECTION$tapping))
}

#' Function to extract tremor features
#' and top 3 most important feature 
#' from previous analysis
get_tremor_features <- function(tremor_id){
    hc_record_mapping <- synTableQuery(
        "SELECT healthCode, createdOn, \
        'answers.medicationTiming' as `medTimepoint`, \
        recordId FROM syn12977322")$asDataFrame() %>%
        dplyr::select(-ROW_ID, -ROW_VERSION) %>%
        tibble::as_tibble()
    top_n_feat <- FEATURE_SELECTION$tremor %>%
        stringr::str_c(collapse = "|")
    synapser::synGet(tremor_id)$path %>%
        fread() %>%
        dplyr::select(-diagnosis, -age, 
                      -sex, -medTimepoint, 
                      -healthCode) %>%
        dplyr::inner_join(
            hc_record_mapping, 
            by = c("recordId")) %>%
        dplyr::group_by(healthCode, createdOn,
                        recordId, activityType,
                        medTimepoint) %>%
        dplyr::select(matches(top_n_feat)) %>%
        dplyr::ungroup()
}


## extract walk feature sets
walk_data <- get_walk_features(
    walk_segment_id = FEATURE_SYN_ID_LIST$walking, 
    rotation_segment_id = FEATURE_SYN_ID_LIST$rotation) %>%
    filter_data() %>%
    dplyr::select(externalId, diagnosis, age, 
                  everything(), -healthCode) %>%
    readr::write_tsv(OUTPUT_FILENAME$walking)
file <- synapser::File(OUTPUT_FILENAME$walking, 
                       parent = PARENT_ID)
synapser::synStore(file, activity = Activity(
    used = c(FEATURE_SYN_ID_LIST$walking, FEATURE_SYN_ID_LIST$rotation),
    executed = NULL))

## extract tap feature sets
tap_data <- get_tap_features(
    FEATURE_SYN_ID_LIST$tapping) %>%
    filter_data() %>%
    dplyr::select(externalId, diagnosis, age, 
                  everything(), -healthCode) %>%
    readr::write_tsv(OUTPUT_FILENAME$tapping)
file <- synapser::File(OUTPUT_FILENAME$tapping, 
                       parent = PARENT_ID)
synapser::synStore(file, activity = Activity(
    used = c(FEATURE_SYN_ID_LIST$tapping),
    executed = NULL))


## extract tremor feature sets
tremor_data <-  get_tremor_features(
    FEATURE_SYN_ID_LIST$tremor)%>%
    filter_data() %>%
    dplyr::select(externalId, diagnosis, age, 
                  everything(), -healthCode) %>%
    readr::write_tsv(OUTPUT_FILENAME$tremor)
file <- synapser::File(
    OUTPUT_FILENAME$tremor, 
    parent = PARENT_ID)
synapser::synStore(file, activity = Activity(
    used = c(FEATURE_SYN_ID_LIST$tremor),
    executed = NULL))

## clean data
purrr::walk(OUTPUT_FILENAME, ~unlink(.x))