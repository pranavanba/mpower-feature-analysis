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

# knit wiki page
knit2synapse::createAndKnitToFolderEntity(
    file = "wiki/generate_tapping_feat_extraction_wiki.rmd",
    parentId = "syn26215070", 
    wikiName = "Tapping",
    folderName = "Tapping"
)

# append here


# clean all cache
clean_cache()
