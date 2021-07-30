save_to_synapse <- function(data, 
                            output_filename, 
                            parent,
                            ...){
    data %>% 
        readr::write_tsv(output_filename)
    file <- File(output_filename, parent =  parent)
    activity <- Activity(...)
    synStore(file, activity = activity)
    unlink(file$path)
}