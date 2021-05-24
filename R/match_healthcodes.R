library(tidyverse)
library(jsonlite)
library(githubr)
library(jsonlite)
library(reticulate)
library(MatchIt)
source("R/utils.R")

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$login()

####################################
#### instantiate github #### 
####################################
GIT_REPO <- "arytontediarjo/feature_extraction_codes"
GIT_TOKEN_PATH <- "~/git_token.txt"
SCRIPT_PATH <- file.path("R", "match_healthcodes.R")
setGithubToken(readLines(GIT_TOKEN_PATH))
GIT_URL <- githubr::getPermlink(GIT_REPO, repositoryPath = SCRIPT_PATH)

####################################
#### instantiate global variables #### 
####################################
OUTPUT_REF <- list(
    walk = list(
        output_file = "age_gender_matched_healthcodes_walk.tsv",
        id = "syn25782772",
        parent_id = "syn25782930"),
    tap = list(
        output_file = "age_gender_matched_healthcodes_tap.tsv",
        id = "syn25782775",
        parent_id = "syn25782930"),
    rest = list(
        output_file ="age_gender_matched_healthcodes_rest.tsv",
        id = "syn25782776",
        parent_id = "syn25782930")
)

## Performs exact age matching for males and females separately,
## and breaks ties by selecting the participants with the 
## largest number of records. (Further ties are broken by 
## randomly selecting tied participants.)
age_gender_matching <- function(dat) {
    GetPairs <- function(mdat) {
        aux <- table(mdat$subclass)
        subclasses <- names(aux)
        nclasses <- length(subclasses)
        cases <- c()
        controls <- c()
        ageCases <- c()
        ageControls <- c()
        for (i in seq(nclasses)) {
            idx0 <- which(mdat$subclass == subclasses[i] & mdat$pd == 0)
            idx1 <- which(mdat$subclass == subclasses[i] & mdat$pd == 1)
            mdat0 <- mdat[idx0,]
            mdat1 <- mdat[idx1,]
            n0 <- length(idx0) ## number of controls
            n1 <- length(idx1) ## number of cases
            
            ## when we have more controls (n0) than cases (n1),
            ## we select all cases, then sort the controls by
            ## number of records (nrecs) and select the top n1
            ## controls
            if (n0 > n1) {
                cases <- c(cases, mdat1$healthCode)
                ageCases <- c(ageCases, mdat1$age)
                mdat0 <- mdat0[order(mdat0$nrecords, decreasing = TRUE),]
                controls <- c(controls, mdat0$healthCode[seq(n1)])
                ageControls <- c(ageControls, mdat0$age[seq(n1)])
            }
            
            ## when we have more cases (n1) than controls (n0),
            ## we select all controls, then sort the cases by
            ## number of records (nrecs) and select the top n0
            ## cases
            if (n1 > n0) {
                controls <- c(controls, mdat0$healthCode)
                ageControls <- c(ageControls, mdat0$age)
                mdat0 <- mdat0[order(mdat0$nrecords, decreasing = TRUE),]
                cases <- c(cases, mdat1$healthCode[seq(n0)])
                ageCases <- c(ageCases, mdat1$age[seq(n0)])
            }
            
            ## when the number of cases is the same as controls
            ## we just select all cases and all controls
            if (n0 == n1) { 
                cases <- c(cases, mdat1$healthCode)
                ageCases <- c(ageCases, mdat1$age)
                controls <- c(controls, mdat0$healthCode)
                ageControls <- c(ageControls, mdat0$age)
            }
            
        }
        healthCode <- c(cases, controls)
        ncases <- length(cases)
        ncontrols <- length(controls)
        pd <- c(rep(1, ncases), rep(0, ncontrols))
        age <- as.numeric(c(ageCases, ageControls))
        idx <- c(seq(ncases), seq(ncontrols))
        data.frame(healthCode, pd, age, idx) 
    }
    dat$pd <- dat$PD
    dat <- dat[, c("healthCode", "pd", "sex", "age", "nrecords")]
    dat <- na.omit(dat)
    datM <- dat[dat$sex == "male",]
    datF <- dat[dat$sex == "female",]
    mM <- MatchIt::matchit(pd ~ age, data = datM, method = "exact")
    mdatM <- MatchIt::match.data(mM)
    hcM <- GetPairs(mdatM)
    hcM$sex <- rep("male", nrow(hcM))
    mF <- MatchIt::matchit(pd ~ age, data = datF, method = "exact")
    mdatF <- MatchIt::match.data(mF)
    hcF <- GetPairs(mdatF)  
    hcF$sex <- rep("female", nrow(hcF))
    return(rbind(hcM, hcF) %>% 
               dplyr::select(everything(), PD = pd))
}


match_healthcodes  <- function(agg_features, plot = FALSE){
    cleaned_features <- agg_features %>% 
        dplyr::filter(
            nrecords >= 5,
            diagnosis != "no_answer",
            age > 0, age <= 120) %>%
        tidyr::drop_na(age, sex, diagnosis) %>%
        dplyr::mutate(PD = ifelse(diagnosis == "parkinsons", 1, 0))
    
    ## perform the matching
    matched_data <- age_gender_matching(cleaned_features)
    
    if(plot == TRUE){
        boxplot(age ~ PD, data = matched_data)
        boxplot(age ~ PD + sex, data = matched_data)
    }
    return(matched_data)
}


purrr::walk(names(OUTPUT_REF), function(activity){
    agg_features <- syn$get(OUTPUT_REF[[activity]])$path %>% fread()
    agg_features %>% 
        match_healthcodes() %>%
        save_to_synapse(
            output_file = OUTPUT_REF[[activity]]$output_file,
            parent_id = OUTPUT_REF[[activity]]$parent_id,
            source_tbl = OUTPUT_REF[[activity]]$id,
            executed = GIT_URL, 
            activity_name = "aggregate features")
})


