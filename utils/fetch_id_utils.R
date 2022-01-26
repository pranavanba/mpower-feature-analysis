get_file_view_ref <- function(syn = NULL){
    template_path <- file.path("synapseformation/manuscript.yaml")
    if(is.null(syn)){
        project_id <- synFindEntityId(
            yaml::read_yaml(template_path)[[1]]$name)
        file_view_id <- synapser::synFindEntityId(
            "mPower2.0 - File View", project_id)
    }else{
        project_id <- syn$findEntityId(
            yaml::read_yaml(template_path)[[1]]$name)
        file_view_id <- syn$findEntityId(
            "mPower2.0 - File View", project_id)
    }
    return(file_view_id)
}

get_feature_extraction_ids <- function(syn = NULL){
    if(is.null(syn)){
        file_view_id <- get_file_view_ref()
        data <- synTableQuery(
            glue::glue("SELECT * FROM {file_view_id}", 
                       file_view_id = file_view_id))$asDataFrame() %>%
            tibble::as_tibble()
    }else{
        file_view_id <- get_file_view_ref(syn)
        data <- syn$tableQuery(
            glue::glue("SELECT * FROM {file_view_id}", 
                       file_view_id = file_view_id))$asDataFrame() %>%
            tibble::as_tibble()
        
    }
    ref_list <- list()
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


get_feature_processed_ids <- function(syn = NULL){
    if(is.null(syn)){
        file_view_id <- get_file_view_ref()
        data <- synTableQuery(
            glue::glue("SELECT * FROM {file_view_id}", 
                       file_view_id = file_view_id))$asDataFrame() %>%
            tibble::as_tibble()
    }else{
        file_view_id <- get_file_view_ref(syn)
        data <- syn$tableQuery(
            glue::glue("SELECT * FROM {file_view_id}", 
                       file_view_id = file_view_id))$asDataFrame() %>%
            tibble::as_tibble()
        
    }
    ref_list <- list()
    ref_list$parent_id <- data %>%
        dplyr::filter(
            type == "folder",
            name == "Features - Processed") %>% .$id
    ref_list$tap_20_secs <- data %>%
        dplyr::filter(
            pipelineStep == "feature processing",
            filter == "20 seconds cutoff",
            task == "tapping",
            analysisType == "tapping-v2") %>% .$id
    ref_list$tap <- data %>%
        dplyr::filter(
            is.na(filter),
            pipelineStep == "feature processing",
            task == "tapping",
            analysisType == "tapping-v2") %>% .$id
    return(ref_list)
}