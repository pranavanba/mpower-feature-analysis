library(reticulate)
library(tidyverse)

DEMO_TBL_V2 <- "syn15673379"
CURRENT_YEAR <- lubridate::year(lubridate::now())

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
        dplyr::arrange(desc(createdOn)) %>%
        distinct(healthCode, .keep_all = TRUE) %>%
        dplyr::rowwise() %>%
        dplyr::mutate(sex = glue::glue_collapse(sex, sep = ","),
                      diagnosis = glue::glue_collapse(diagnosis, sep = ",")) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(healthCode) %>%
        dplyr::summarise(age = mean(age, na.rm = TRUE),
                         sex = first(sex),
                         diagnosis = first(diagnosis),
                         phoneInfo = first(phoneInfo),
                         operatingSystem = first(operatingSystem))
    return(demo)
}

demo_v2 <- get_demographics_v2()
