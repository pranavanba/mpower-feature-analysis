#' function to segment gait data
segment_gait_data <- function(data){
  result <- list()
  data <- data %>% 
    dplyr::filter(is.na(error))
  result$rotation_segment <- data %>% 
    dplyr::filter(!is.na(rotation_omega))
  result$walk_segment <- data %>% 
    dplyr::filter(is.na(rotation_omega)) 
  return(result)
}

clean_medication_timing_cols <- function(data){
  data <- data %>%
    dplyr::mutate(
      operatingSystem = ifelse(stringr::str_detect(phoneInfo, "iOS"), "iOS", "Android"))
  if("answers.medicationTiming" %in% names(data)){
    data <- data %>% 
      dplyr::rowwise() %>%
      dplyr::mutate(medTimepoint = glue::glue_collapse(answers.medicationTiming, ", "))
  }else{
    data <- data %>% 
      dplyr::mutate(medTimepoint = NA)
  }
  return(data)
}

clean_phone_info <- function(data){
  data %>%
    dplyr::mutate(
      operatingSystem = ifelse(stringr::str_detect(phoneInfo, "iOS"), "iOS", "Android"))
}
