library(knit2synapse)
synapser::synLogin()

#' function to clean wiki generation cache from knit2synapse
clean_cache <- function(){
    list.files("wiki") %>% 
        purrr::walk(function(file){
            if(stringr::str_detect(file, "_cache$")){
                unlink(file.path("wiki", file), recursive = T)
            }
        }) 
}

# append here
knit2synapse::createAndKnitToFileEntity(
    file = "analysis/tap_features_diagnostics.Rmd",
    parentId = "syn26411005", 
    fileName = "Tapping-Diagnostics"
)


# clean all cache
clean_cache()
