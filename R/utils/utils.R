save_to_synapse <- function(data, 
                            output_filename, 
                            parent,
                            provenance = FALSE, ...){
    data %>% 
        readr::write_tsv(output_filename)
    file <- File(output_filename, parent =  parent)
    
    if(provenance){
        provenance_param = list(...)
        activity = Activity(
            name = provenance_param$name,
            used = provenance_param$used,
            executed = provenance_param$executed)
        
    }else{
        activity = Activity()
    }
    store_to_synapse <- synStore(file, activity = activity)
    unlink(output_filename)
}