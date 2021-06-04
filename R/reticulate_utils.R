#' function to check whether medication timepoint exist
parse_medTimepoint <- function(data){
  if("answers.medicationTiming" %in% names(data)){
    data %>% 
      dplyr::rowwise() %>%
      dplyr::mutate(medTimepoint = glue::glue_collapse(answers.medicationTiming, ", ")) %>%
      dplyr::ungroup()
  }else if("medTimepoint" %in% names(data)){
    data %>% 
      dplyr::mutate(medTimepoint = unlist(medTimepoint))
  }else{
    data %>% 
      dplyr::mutate(medTimepoint = NA)
  }
}

#' function to parse phone information
parse_phoneInfo <- function(data){
  data %>%
    dplyr::mutate(
      operatingSystem = ifelse(str_detect(phoneInfo, "iOS|iPhone"), "iOS", "Android"))
}

get_table <- function(syn, synapse_tbl, file_columns, uid, keep_metadata){
  # get table entity
  entity <- syn$tableQuery(glue::glue("SELECT * FROM {synapse_tbl} LIMIT 50"))
  
  # shape table
  table <- entity$asDataFrame() %>%
    tibble::as_tibble(.) %>%
    tidyr::pivot_longer(cols = all_of(file_columns), 
                        names_to = "fileColumnName", 
                        values_to = "fileHandleId") %>%
    dplyr::mutate(across(everything(), unlist)) %>%
    dplyr::filter(!is.na(fileHandleId)) %>%
    dplyr::group_by(recordId, fileColumnName) %>% 
    dplyr::summarise_all(last) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(
      createdOn = as.POSIXct(createdOn/1000, 
                             origin="1970-01-01"),
      fileHandleId = as.character(fileHandleId))
  
  # download all table columns
  result <- syn$downloadTableColumns(
    table = entity, 
    columns = file_columns) %>%
    tibble::enframe(.) %>%
    tidyr::unnest(value) %>%
    dplyr::select(
      fileHandleId = name, 
      filePath = value) %>%
    dplyr::mutate(filePath = unlist(filePath)) %>%
    dplyr::right_join(table, by = c("fileHandleId"))
  return(result)
}

save_to_synapse <- function(syn,
                            synapseclient,
                            data, 
                            output_filename, 
                            parent,
                            provenance = NULL){
  data %>% 
    readr::write_tsv(output_filename)
  file <- synapseclient$File(output_filename, parent = parent)
  
  if(!is.null(provenance)){
    activity <- provenance %>% 
      purrr::pmap(synapseclient$Activity) %>% 
      purrr::reduce(c)
    
  }else{
    activity <- synapseclient$Activity()
  }
  store_to_synapse <- syn$store(file, activity = activity)
  unlink(output_filename)
}

