library(tidyverse)
library(data.table)
library(synapser)
library(synapserutils)
library(githubr)

synLogin()

####################################
#' Git Reference
####################################
GIT_TOKEN_PATH <- "~/git_token.txt"
GIT_REPO <- "arytontediarjo/mpower-feature-analysis"
SCRIPT_PATH <- "R/curate_tables/generate_custom_walk_tbls.R"
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(
    repository = GIT_REPO, 
    repositoryPath = SCRIPT_PATH,
    ref = "branch",
    refName = 'master')

####################################
#' Globals
####################################
PARENT_SYN_ID <- "syn24182618"
KEEP_COLS <- c("recordId", 
               "createdOn", 
               "healthCode", 
               "phoneInfo")
NEW_COLS <- list(
    Column(name = "diagnosis", columnType = "STRING", maximumSize = 40),
    Column(name = "walking", columnType = "DOUBLE"))
FILE_HANDLE_COLS <- c("walk_motion.json")
TBL_REF <- list(
    active_v2 = list(
        id = "syn12514611",
        output_tbl_name = "WalkActivity_V2-PredictionScore",
        pred_prob = "syn25791905",
        fh_cols = c("walk_motion.json"),
        demo = "syn25421202"
    ),
    passive = list(
        id = "syn17022539" ,
        output_tbl_name = "PassiveWalk-PredictionScore",
        pred_prob = "syn25791906",
        fh_cols = c("walk_motion.json"),
        demo = "syn25421202"
    ),
    active_v1 = list(
        id = "syn10308918",
        output_tbl_name = "WalkActivity_V1-PredictionScore",
        pred_prob = "syn25981276",
        fh_cols = c("accel_walking_outbound.json.items", 
                    "deviceMotion_walking_outbound.json.items"),
        demo = "syn25782458"
    )
)

get_pred_prob <- function(pred_prob){
    data <- fread(
        synGet(pred_prob)$path)
    if("V1" %in% names(data)){
        data <- data %>% 
            dplyr::select(recordId = V1)
    }
    return(data)
}

get_merged_demo_mpower_data <- function(tbl_id, demo_id){
    demographics <- fread(synGet(demo_id)$path)
    synTableQuery(glue::glue(
        "SELECT * FROM {tbl_id}"))$asDataFrame() %>%
        dplyr::left_join(
            demographics %>% 
                dplyr::select(healthCode, diagnosis), 
            by = c("healthCode"))
}

check_existence <- function(name, parent){
    tryCatch({
        synapser::synFindEntityId(name, parent)
    }, error = function(e){
        NULL
    })
}

get_source_column_schema <- function(table, cols_subset = NULL){
    col_obj <- synapser::synGetColumns(table)$asList()
    if(is.null(cols_subset)){
        return(col_obj)
    }else{
        col_obj <- purrr::map(
            col_obj,
            function(col_obj){
                if(col_obj$name %in% cols_subset){
                    return(col_obj)}}) %>%
            purrr::reduce(c)
        return(col_obj)
    }
}

add_additional_columns <- function(cols, new_cols = NULL){
    c(cols, new_cols)
}

copy_file_handles <- function(data, tbl_id, file_handle_cols){
    all_data <- data %>%
        tidyr::pivot_longer(all_of(file_handle_cols), 
                            names_to = "file_column_name", 
                            values_to = "file_handle_id") %>%
        dplyr::mutate(associateObjectTypes = "TableEntity",
                      associateObjectIds = tbl_id,
                      contentTypes = "json",
                      fileNames = NA)
    map_data <- all_data %>% tidyr::drop_na(file_handle_id)
    new_filehandle_map <- synapserutils::copyFileHandles(
        fileHandles = map_data$file_handle_id,
        associateObjectTypes = map_data$associateObjectTypes,
        associateObjectIds = map_data$associateObjectIds,
        contentTypes = map_data$contentTypes,
        fileNames = map_data$fileNames) %>% 
        purrr::map_dfr(., function(fhandle){
            tibble::tibble(
                new_file_handle_id = fhandle$newFileHandle$id,
                original_file_handle_id = fhandle$originalFileHandleId)
        })
    result <- all_data %>% 
        dplyr::left_join(
            new_filehandle_map, 
            by = c("file_handle_id" = "original_file_handle_id")) %>%
        tidyr::pivot_wider(
            id_cols = c("recordId", 
                        "diagnosis", 
                        "healthCode",
                        "walking",
                        "createdOn",
                        "appVersion", 
                        "phoneInfo"),
            names_from = "file_column_name", 
            values_from = "new_file_handle_id")
    return(result)
}

regenerate_table <- function(data, 
                             tbl_name, 
                             parent, keep_cols, 
                             file_handle_cols = NULL,
                             src_tbl_id = NULL,
                             new_cols = NULL,
                             provenance = FALSE,
                             ...){
    # check if table if exist (if yes, delete rows)
    id <- check_existence(tbl_name, parent)
    
    # check if id is not null make table
    if(!is.null(id)){
        schema <- synapser::synGet(id)
        if(schema$has_columns()){
            synapser::synGetColumns(
                schema$properties$id)$asList() %>% 
                purrr::walk(function(col){
                    schema$removeColumn(col)
                })
            synStore(schema)
        }
    }
    
    # rebuild column from source
    col_obj <- get_source_column_schema(
        src_tbl_id,
        cols_subset =  c(keep_cols, file_handle_cols)) %>% 
        add_additional_columns(new_cols = new_cols)
    schema <- Schema(name = tbl_name,
                     columns = col_obj,
                     parent = parent)
    schema <- synStore(schema)
    col_used_in_schema <- col_obj %>% 
        purrr::map(~.x$name) %>% 
        purrr::reduce(c)
    
    # copy file handles when prompted
    if(!is.null(file_handle_cols)){
        data <- copy_file_handles(
            data = data,
            tbl_id = src_tbl_id,
            file_handle_cols = file_handle_cols)
    }
    
    # set provenance 
    if(provenance){
        parameter <- list(...)
        activity <- Activity(
            name = parameter$name,
            used = src_tbl_id,
            executed = parameter$git_url)
        synSetProvenance(schema$properties$id, activity)
    }
    
    # match data with schema
    data <- data %>%
        dplyr::select(all_of(col_used_in_schema))
    table <- Table(schema$properties$id, data)
    synStore(table)
}

main <- function(){
    purrr::map(names(TBL_REF), function(activity){
        pred_prob <- get_pred_prob(TBL_REF[[activity]]$pred_prob)
        activity_data <- get_merged_demo_mpower_data(
            tbl_id = TBL_REF[[activity]]$id,
            demo_id = TBL_REF[[activity]]$demo) %>% 
            dplyr::inner_join(pred_prob, by = c("recordId"))
        regenerate_table(
            data = activity_data, 
            tbl_name = TBL_REF[[activity]]$output_tbl_name, 
            parent = PARENT_SYN_ID, 
            keep_cols = KEEP_COLS,
            file_handle_cols = TBL_REF[[activity]]$fh_cols,
            src_tbl_id = TBL_REF[[activity]]$id,
            new_cols = NEW_COLS,
            provenance = TRUE,
            name = "extract custom table",
            git_url = GIT_URL)
    })
}

main()









