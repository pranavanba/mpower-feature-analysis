###############################################################
#' Collection of script to clean synapse table metadata and 
#' raw sensor features
#' 
#' @author: aryton.tediarjo@sagebase.org
####################################################################

#' Function to normalize timestamp
#' by detecting whether median of the
#' timestamp is in ms and then convert 
#' it into seconds
#' 
#' @param data dataframe
#' @return time-series in seconds interval
normalize_timestamp <- function(data){
    if(median(data$t) > 1000){
        data <- data %>% 
            dplyr::mutate(t = t/1000)
    }
    return(data %>%
               dplyr::arrange(t))
}

#' Function to curate app version
#' parse appVersion to version and build
curate_app_version <- function(data){
    data %>% 
        tidyr::separate(appVersion, 
                        ",", 
                        into = c("version", "build"), 
                        remove = FALSE) %>%
        dplyr::select(-appVersion)
}

#' Function to clean group versions
curate_version_group <- function(data){
    data %>%
        dplyr::mutate(version_group = case_when(
            stringr::str_detect(version, "version 0.") ~ "android",
            stringr::str_detect(version, "version 1.0") ~ "version 1.0",
            stringr::str_detect(version, "version 1.1") ~ "version 1.1",
            stringr::str_detect(version, "version 1.2") ~ "version 1.2",
            stringr::str_detect(version, "version 1.3") ~ "version 1.3",
            stringr::str_detect(version, "version 1.4") ~ "version 1.4",
            stringr::str_detect(version, "version 2.0") ~ "version 2.0",
            stringr::str_detect(version, "version 2.1") ~ "version 2.1",
            stringr::str_detect(version, "version 2.2") ~ "version 2.2",
            stringr::str_detect(version, "version 2.3") ~ "version 2.3",
            TRUE ~ "others"
        ))
}


#' Function to parse medTimepoint
curate_med_timepoint <- function(data){
    if("answers.medicationTiming" %in% names(data)){
        data %>% 
            dplyr::select(everything(), 
                          medTimepoint := answers.medicationTiming)
    }else{
        data
    }
}

# Function to simplify phoneInformation
curate_phone_info <- function(data){
    data %>%
        dplyr::mutate(phoneInfo = case_when(
            str_detect(tolower(phoneInfo), "iphone.4|iphone4") ~ "iPhone 4",
            str_detect(tolower(phoneInfo), "iphone.5|iphone5") ~ "iPhone 5",
            str_detect(tolower(phoneInfo), "iphone.6|iphone6") ~ "iPhone 6",
            str_detect(tolower(phoneInfo), "iphone.7|iphone7") ~ "iPhone 7",
            str_detect(tolower(phoneInfo), "iphone.8|iphone8") ~ "iPhone 8",
            str_detect(tolower(phoneInfo), "iphone.9|iphone9") ~ "iPhone 9",
            str_detect(tolower(phoneInfo), "iphone.10|iphone10|iphonex|iphone.x") ~ "iPhone 10",
            str_detect(tolower(phoneInfo), "iphone.11|iphone11") ~ "iPhone 11",
            str_detect(tolower(phoneInfo), "iphone.12|iphone12") ~ "iPhone 12",
            str_detect(tolower(phoneInfo), "iphone.13|iphone13") ~ "iPhone 13",
            str_detect(tolower(phoneInfo), "iphone.se|iphonese") ~ "iPhone SE",
            str_detect(tolower(phoneInfo), "samsung|google|motorola|android|lge|htc|huawei|android|oneplus|nokia") ~ "Android",
            str_detect(tolower(phoneInfo), "ipod") ~ "iPod",
            str_detect(tolower(phoneInfo), "ipad") ~ "iPad",
            str_detect(tolower(phoneInfo), "simulator") ~ "Simulator",
            TRUE ~ "Others"
        ))
}

#' Function to remove test users
remove_test_user <- function(data){
    test_user <- data %>%
        dplyr::filter(str_detect(dataGroups, "test_user")) 
    data %>% 
        dplyr::anti_join(test_user, by = c("recordId")) %>%
        dplyr::select(-dataGroups)
}

#' Function to parse strings into comma-separated string
vectorise_optparse_string <- function(opt_string){
    opt_string %>%
        stringr::str_replace_all(" ", "") %>%
        stringr::str_split(",") %>%
        purrr::reduce(c)
}

#' Function to parse accelerometer data
parse_accel_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(timestamp = timestamp - .$timestamp[1]) %>%
        dplyr::select(t = timestamp, x, y, z)
}

#' Function to parse rotation rate data
parse_rotation_data <- function(filePath){
    jsonlite::fromJSON(filePath) %>%
        dplyr::mutate(t = timestamp - .$timestamp[1],
                      x = .$rotationRate$x,
                      y = .$rotationRate$y,
                      z = .$rotationRate$z) %>% 
        dplyr::select(t, x, y, z)
}
