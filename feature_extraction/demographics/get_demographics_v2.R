library(reticulate)
library(tidyverse)
library(githubr)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

DEMO_TBL_V2 <- "syn15673379"
CURRENT_YEAR <- lubridate::year(lubridate::now())

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("feature_extraction", 
                         "demographics",
                         "get_demographics.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)
OUTPUT_FILE <- "extracted_demographics_v2.tsv"
OUTPUT_PARENT_ID <- "syn26601399"
diagnosis_levels <- c("no_answer", "control", "parkinsons")
sex_levels <- c("no_answer", "male", "female")

#' get demographic info, average age for multiple records
#' get most recent entry for sex, createdOn, diagnosis
#' remove test users
get_demographics_v2 <- function(){
    demo <- syn$tableQuery(
        glue::glue("SELECT * FROM {DEMO_TBL_V2}"))$asDataFrame() %>% 
        tibble::as_tibble() %>% 
        dplyr::filter(!str_detect(dataGroups, "test")) %>%
        dplyr::mutate(age = CURRENT_YEAR - birthYear,
                      createdOn = as.POSIXct(createdOn/1000, origin="1970-01-01"),
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

save_to_synapse <- function(data){
    write_file <- readr::write_tsv(data, OUTPUT_FILE)
    file <- synapseclient$File(
        OUTPUT_FILE, 
        parent=OUTPUT_PARENT_ID)
    activity <- synapseclient$Activity(
        "extract demographics for mPower V2", 
        executed = GIT_URL,
        used = c(DEMO_TBL_V2))
    syn$store(file, activity = activity)
    unlink(OUTPUT_FILE)
}


main <- function(){
    demo_v2 <- get_demographics_v2() %>% 
        save_to_synapse()
}

main()
