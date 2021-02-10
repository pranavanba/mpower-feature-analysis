library(data.table)
library(argparse)
library(tidyverse)
library(githubr)
library(plyr)

####################################
#### Parsing ##############
####################################
#' function to parse argument used for extracting features
parse_argument <- function(){
    parser <- ArgumentParser()
    parser$add_argument("-g", 
                        "--git_token", 
                        type="character", 
                        default="~/git_token.txt", 
                        help="path to github token")
    parser$add_argument("-r", 
                        "--git_repo", 
                        type="character", 
                        default="arytontediarjo/feature_extraction_codes", 
                        help="path to cloned/fork github repo")
    parser$add_argument("-f", 
                        "--features", 
                        type="character", 
                        default="syn24182637", 
                        help="synId feature source")
    parser$add_argument("-o", 
                        "--output", 
                        type="character", 
                        default="PDkitRotation_walk30s_features_mPowerV2_userAgg.tsv", 
                        help="synId table source")
    parser$add_argument("-p", 
                        "--parent_id", 
                        type="character", 
                        default="syn24184522", 
                        help="synapse parent id")
    parser$add_argument("-agg", 
                        "--run_aggregate", 
                        action="store_true", 
                        help="whether to aggregate features")
    parser$add_argument("-d", 
                        "--demo_version", 
                        type = "integer",
                        default = 2,
                        help= "which demographics table")
    return(parser$parse_args())
}

parsed_var <- parse_argument()
FEATURES <- parsed_var$features
GIT_TOKEN_PATH <- parsed_var$git_token
GIT_REPO <- parsed_var$git_repo
SCRIPT_PATH <- file.path("R", "aggregate_features.R")
OUTPUT_FILE <- parsed_var$output
OUTPUT_PARENT_ID <- parsed_var$parent_id
DO_AGGREGATE <- parsed_var$run_aggregate
DEMO_TBL_VERSION <- parsed_var$demo_version
USER_CATEGORIZATION <- "syn17074533"

synapser::synLogin()


##############################
# Global Variables
#############################
DEMO_TBL_V1 <- "syn10371840"
DEMO_TBL_V2 <- "syn15673379"
CURRENT_YEAR  <- lubridate::year(lubridate::now())

####################################
#### instantiate github #### 
####################################
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)


##############################
# Helpers
#############################

#' get user categorization 
get_user_categorization <- function(){
    fread(synapser::synGet(USER_CATEGORIZATION)$path, sep = ",") %>% 
        tibble::as_tibble(.) %>% 
        dplyr::filter(userType != "test")
}

#' utility function for aggregating features
aggregate_sensor_features <- function(data, col = c("healthCode"), 
                                      agg_func = list("med" = median, "iqr" = IQR,"var" = var)){
    num_records <- data %>%
        dplyr::group_by(healthCode) %>%
        dplyr::summarise(nrecords = n()) %>%
        dplyr::select(healthCode, nrecords)
    features <- data %>%
        dplyr::select(-c("window_end", "window_start", "window_end")) %>%
        dplyr::group_by_at(col) %>%
        dplyr::summarise_if(is.numeric, .funs = agg_func, na.rm = TRUE)
    aggregate_data <- num_records %>% dplyr::inner_join(features, by = c("healthCode"))
    return(aggregate_data)
}

#' function to retrieve/clean demographic information
#' of mPower users
get_demographics_v1 <- function(){
    demo <- synapser::synTableQuery(
        glue::glue("SELECT * FROM {DEMO_TBL_V1}"))$asDataFrame()
    colnames(demo) <- gsub("_|-", ".", names(demo))
    if("inferred.diagnosis" %in% names(demo)){
        demo <- demo %>% 
            mutate(diagnosis = demo$inferred.diagnosis) %>%
            dplyr::select(-c(professional.diagnosis, inferred.diagnosis)) %>%
            filter(dataGroups %in% c("parkinson", "control", NA))
    }else{
        demo <- demo %>% 
            dplyr::rename("diagnosis" = "professional.diagnosis")
    }
    ## clean demographics data
    demo <- demo %>%
        dplyr::select(healthCode, age, sex = gender, diagnosis) %>%
        dplyr::mutate(sex = tolower(sex)) %>%
        dplyr::filter((!is.infinite(age) & age <= 120) | is.na(age)) %>%
        plyr::ddply(.(healthCode), .fun = function(x){
            x$age = mean(x$age, na.rm = TRUE)
            x$diagnosis = ifelse(
                length(unique(x$diagnosis)) > 1, 
                NA, unique(x$diagnosis))
            return(x[1,])})
    return(demo)
}

#' get demographic info, average age for multiple records
#' get most recent entry for sex, createdOn, diagnosis
#' remove test users
get_demographics_v2 <- function(){
    synapser::synTableQuery(
        glue::glue("SELECT * FROM {DEMO_TBL_V2}"))$asDataFrame() %>% 
        as_tibble() %>% 
        dplyr::filter(!str_detect(dataGroups, "test")) %>%
        dplyr::mutate(age = CURRENT_YEAR - birthYear) %>%
        dplyr::arrange(desc(createdOn)) %>%
        distinct(healthCode, .keep_all = TRUE) %>%
        dplyr::group_by(healthCode, diagnosis, sex) %>% 
        dplyr::summarise(age = mean(age, na.rm = TRUE)) %>% 
        dplyr::mutate(
            diagnosis = ifelse(
                diagnosis == "no_answer", NA, diagnosis),
                sex = ifelse(sex == "no_answer", NA, sex))
}

main <- function(){
    #' parsing condition
    if(parsed_var$demo_version == 1){
        demo_syn_id <- DEMO_TBL_V1
        demographics_data <- get_demographics_v1()
    }else{
        demo_syn_id <- DEMO_TBL_V2
        demographics_data <- get_demographics_v2() %>% 
            dplyr::inner_join(
                get_user_categorization(), 
                by = c("healthCode"))
    }
    
    #' retrieve and aggregate features
    if(DO_AGGREGATE){
        result <- fread(synapser::synGet(FEATURES)$path) %>%
            aggregate_sensor_features(.) %>%
            dplyr::inner_join(demographics_data, 
                              by = c("healthCode"))
    }else{
        result <- fread(synapser::synGet(FEATURES)$path) %>%
            dplyr::inner_join(demographics_data, 
                              by = c("healthCode"))
    }
    write.table(result, OUTPUT_FILE, sep = "\t", row.names=F, quote=F)
    f <- synapser::File(OUTPUT_FILE, OUTPUT_PARENT_ID)
    synapser::synStore(
        f, activity = synapser::Activity(
        "aggregate walk features",
        used = c(FEATURES, demo_syn_id),
        executed = GIT_URL))
    unlink(OUTPUT_FILE)
}

main()





