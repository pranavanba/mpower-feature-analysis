library(reticulate)
library(tidyverse)
library(githubr)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

DEMO_TBL_V1 <- "syn10371840"
CURRENT_YEAR <- lubridate::year(lubridate::now())

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "get_demographics_v1.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)
OUTPUT_FILE <- "demographics_v1.tsv"
OUTPUT_PARENT_ID <- "syn25756375"
diagnosis_levels <- c("no_answer", "control", "parkinsons")
sex_levels <- c("no_answer", "male", "female")


#' get demographic info, average age for multiple records
#' get most recent entry for sex, createdOn, diagnosis
#' remove test users
get_demographics_v1 <- function(){
    demo <- syn$tableQuery(
        glue::glue("SELECT * FROM {DEMO_TBL_V1}"))$asDataFrame() %>% 
        tibble::as_tibble() %>% 
        dplyr::filter(!str_detect(dataGroups, "test")) %>%
        dplyr::mutate(age,
                      operatingSystem = ifelse(str_detect(phoneInfo, "iOS|iPhone"), 
                                               "ios", "android")) %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
            sex = tolower(glue::glue_collapse(gender, sep = ",")),
            education = glue::glue_collapse(education, sep = ","),
            diagnosis = glue::glue_collapse(inferred_diagnosis, sep = ","),
            diagnosis = case_when(diagnosis == TRUE ~ "parkinsons", 
                                  diagnosis == FALSE ~ "control",
                                  TRUE ~ "no_answer")) %>%
        dplyr::ungroup() %>%
        dplyr::select(healthCode, 
                      age, 
                      education,
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
                         education = last(education),
                         diagnosis = max(diagnosis),
                         phoneInfo = last(phoneInfo),
                         operatingSystem = last(operatingSystem))
    return(demo)
}


save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        "extract demographics for mPower V1", 
        executed = GIT_URL,
        used = c(DEMO_TBL_V1))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}

demo <- get_demographics_v1() %>% save_to_synapse()
