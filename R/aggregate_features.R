library(data.table)
library(argparse)
library(tidyverse)

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
    return(parser$parse_args())
}

parsed_var <- parse_argument()
FEATURES <- parsed_var$features
PYTHON_ENV <- parsed_var$venv_path
GIT_TOKEN_PATH <- parsed_var$git_token
GIT_REPO <- parsed_var$git_repo
OUTPUT_FILE <- parsed_var$output
OUTPUT_PARENT_ID <- parsed_var$parent_id
FILEHANDLE <- parsed_var$filehandle
DO_AGGREGATE <- parsed_var$run_aggregate

synapser::synLogin()


##############################
# Global Variables
#############################
FEATURES <- "syn24182646"
DEMO_TBL <- "syn15673379"
CURRENT_YEAR  <- lubridate::year(lubridate::now())


##############################
# Helpers
#############################
#' utility function for aggregating features
aggregate_sensor_features <- function(data, col = c("healthCode"), 
                                      agg_func = list("med" = median, "iqr" = IQR,"var" = var)){
    data %>%
        dplyr::select(-c("window_end", "window_start", "window_end")) %>%
        dplyr::group_by_at(col) %>%
        dplyr::summarise_if(is.numeric, .funs = agg_func, na.rm = TRUE)
}

#' get demographic info, average age for multiple records
#' get most recent entry for sex, createdOn, diagnosis
#' remove test users
get_demographics <- function(){
    synapser::synTableQuery(
        glue::glue("SELECT * FROM {DEMO_TBL}"))$asDataFrame() %>% 
        as_tibble() %>% 
        dplyr::filter(!str_detect(dataGroups, "test")) %>%
        dplyr::mutate(age = CURRENT_YEAR - birthYear) %>%
        dplyr::arrange(desc(createdOn)) %>%
        distinct(healthCode, .keep_all = TRUE) %>%
        group_by(healthCode, diagnosis, sex) %>% 
        summarise(age = mean(age, na.rm = TRUE))
}


main <- function(){
    #' retrieve and aggregate features
    if(DO_AGGREGATE){
        result <- fread(synapser::synGet(FEATURES)$path) %>%
            aggregate_sensor_features(.) %>%
            dplyr::inner_join(get_demographics(), by = c("healthCode"))
    }else{
        result <- fread(synapser::synGet(FEATURES)$path) %>%
            dplyr::inner_join(get_demographics(), by = c("healthCode"))
    }
    write.table(result, OUTPUT_FILE, sep = "\t", row.names=F, quote=F)
    f <- sc$File(OUTPUT_FILE, OUTPUT_PARENT_ID)
    syn$store(f, activity = sc$Activity(
        "aggregate walk features",
        used = c(WALK_TBL),
        executed = GIT_URL))
    unlink(OUTPUT_FILE)
}





