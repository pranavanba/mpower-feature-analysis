get_file_view_ref <- function(){
    template_path <- file.path("synapseformation/manuscript.yaml")
    project_id <- synFindEntityId(
        yaml::read_yaml(template_path)[[1]]$name)
    file_view_id <- synapser::synFindEntityId(
        "mPower2.0 - File View", project_id)
    return(file_view_id)
}

get_feature_extraction_ids <- function(){
    file_view_id <- get_file_view_ref()
    ref_list <- list()
    data <- synTableQuery(
        glue::glue("SELECT * FROM {file_view_id}", 
                   file_view_id = file_view_id))$asDataFrame() %>%
        tibble::as_tibble()
    ref_list$parent_id <- data %>%
        dplyr::filter(
            type == "folder",
            name == "Features - Extracted") %>% .$id
    ref_list$demo <- data %>%
        dplyr::filter(
            analysisType == "demographics-v2",
            pipelineStep == "feature extraction") %>% .$id
    ref_list$tap_20_secs <- data %>%
        dplyr::filter(
            pipelineStep == "feature extraction",
            filter == "20 seconds cutoff",
            task == "tapping",
            analysisType == "tapping-v2") %>% .$id
    ref_list$tap <- data %>%
        dplyr::filter(
            is.na(filter),
            pipelineStep == "feature extraction",
            task == "tapping",
            analysisType == "tapping-v2") %>% .$id
    ref_list$tremor <- data %>%
        dplyr::filter(
            is.na(filter),
            pipelineStep == "feature extraction",
            task == "tremor",
            analysisType == "tremor-v2") %>% .$id
    return(ref_list)
}


get_feature_processed_ids <- function(){
    file_view_id <- get_file_view_ref()
    ref_list <- list()
    data <- synTableQuery(
        glue::glue("SELECT * FROM {file_view_id}", 
                   file_view_id = file_view_id))$asDataFrame() %>%
        tibble::as_tibble()
    ref_list$parent_id <- data %>%
        dplyr::filter(
            type == "folder",
            name == "Features - Processed") %>% .$id
    # ref_list$tap_20_secs <- data %>%
    #     dplyr::filter(
    #         filter == "20 seconds cutoff",
    #         task == "tapping",
    #         analysisType == "tapping-v2") %>% .$id
    # ref_list$tap <- data %>%
    #     dplyr::filter(
    #         is.na(filter),
    #         task == "tapping",
    #         analysisType == "tapping-v2") %>% .$id
    return(ref_list)
}
