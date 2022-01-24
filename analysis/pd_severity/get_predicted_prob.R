library(randomForest)
library(data.table)
library(synapser)
library(githubr)
library(tidyverse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
synLogin()

# read config
CONFIG_PATH <- "templates/config.yaml"
ref <- config::get(file = CONFIG_PATH)

# load matched HC
load(synGet("syn15355694")$path)

# demographics of superPD users
demo_id <- synapser::synFindEntityId(
    name = ref$demo$baseline_superusers$output_filename,
    parent = ref$demo$baseline_superusers$parent_id
)

# train-test mapping
train_test_mapping <- list(
    tapping = list(train = "syn25782775", 
                   test = "syn26612333"), 
    walking = list(train = "syn25782772", 
                   test = "syn26619783")
)

# Global Variables
git_url <- get_github_url(
    git_token_path = ref$git_token_path,
    git_repo = ref$repo_endpoint,
    script_path = "analysis/pd_severity/get_predicted_prob.R")




GetRfPrediction <- function(dat.train, 
                            dat.test, 
                            label.name, 
                            feature.names,
                            pos.class.name) {
    set.seed(1000)
    dat.train <- dat.train[, c(label.name, feature.names)]
    dat.train <- na.omit(dat.train)
    dat.test <- dat.test[, c(feature.names)]
    dat.test <- na.omit(dat.test)  
    my.formula <- as.formula(paste(label.name, " ~ ", paste(feature.names, collapse = " + "), sep = ""))
    fit <- randomForest(my.formula, dat.train, ntree = 1000)
    pred.probs <- predict(fit, newdata = dat.test[, feature.names], type = "prob")[, pos.class.name]
    
    pred.probs
}


GetCollapsedFeaturesmPower2 <- function(dat, featNames) {
    ids <- na.omit(as.character(unique(dat$healthCode))) 
    nids <- length(ids)
    nfeat <- length(featNames)
    out <- data.frame(matrix(NA, nids, 2*nfeat + 1))
    colnames(out) <- c("healthCode", paste(featNames, "md", sep = "_"), paste(featNames, "iqr", sep = "_"))
    rownames(out) <- ids
    for (i in seq(nids)) {
        #cat(i, "\n")
        sdat <- dat[which(dat$healthCode == ids[i]),]
        out[i, "healthCode"] <- ids[i]
        out[i, 2:(nfeat+1)] <- apply(sdat[, featNames], 2, median, na.rm = TRUE)
        out[i, (nfeat+2):(2*nfeat+1)] <- apply(sdat[, featNames], 2, IQR, na.rm = TRUE)
    }
    
    out
}


###########################################
#' Get training tap data
###########################################
tap.train <- read.delim(synGet(train_test_mapping$tapping$train)$path, 
                        header = TRUE, quote = "")
tap.train <- tap.train[tap.train$healthCode %in% tapHC,] %>%
    dplyr::mutate(diagnosis = factor(
        diagnosis, 
        levels = c("control", "parkinsons")))
tap.features <- tap.train %>%
    dplyr::select(matches("iqr|md")) %>%
    names() %>%
    purrr::map_chr(function(x){
        str_remove(x, "_iqr|_md")
    }) %>% unique()

tap.test <- fread(synGet(train_test_mapping$tapping$test)$path)
tap.test <- tap.test %>%
    dplyr::select(recordId, healthCode, all_of(tap.features)) %>%
    as.data.frame(.)
tap.test <- GetCollapsedFeaturesmPower2(
    dat = tap.test, featNames = tap.features)
feature.names.tap <- intersect(names(tap.train), names(tap.test))
pred.tap.rf <- GetRfPrediction(dat.train = tap.train, 
                               dat.test = tap.test, 
                               label.name = "diagnosis", 
                               feature.names = feature.names.tap,
                               pos.class.name = "parkinsons") %>%
    tibble::enframe() %>%
    dplyr::select(healthCode = name, tap = value)


###########################################
#' Get training Walk data
###########################################
wal.train <- read.delim(synGet(train_test_mapping$walking$train)$path, 
                        header = TRUE, quote = "")
wal.train <- wal.train[wal.train$healthCode %in% walHC,] %>%
    dplyr::mutate(diagnosis = factor(
        diagnosis, 
        levels = c("control", "parkinsons")))
wal.features <- wal.train %>%
    dplyr::select(
        matches("iqr|md"), 
        -matches("rotation")) %>%
    names() %>%
    purrr::map_chr(function(x){
        str_remove(x, "_iqr|_md")
    }) %>% unique()

wal.test <- fread(synGet(train_test_mapping$walking$test)$path) %>%
    dplyr::select(recordId, 
                  healthCode, 
                  all_of(wal.features)) %>%
    as.data.frame(.)
wal.test <- GetCollapsedFeaturesmPower2(
    dat = wal.test, 
    featNames = wal.features)
feature.names.wal <- intersect(names(wal.train), names(wal.test))
pred.wal.rf <- GetRfPrediction(dat.train = wal.train, 
                               dat.test = wal.test, 
                               label.name = "diagnosis", 
                               feature.names = feature.names.wal,
                               pos.class.name = "parkinsons") %>%
    tibble::enframe() %>%
    dplyr::select(healthCode = name, walk = value)



###########################################
#' Merge results
###########################################
demo <- fread(synGet(demo_id)$path)
pred_list <- list(pred.tap.rf, pred.wal.rf)
pd.severity <- pred_list %>%
    purrr::reduce(dplyr::full_join) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
        mean_conf_score = mean(
            c_across(where(is.numeric)), 
            na.rm = T)) %>%
    dplyr::ungroup() %>%
    dplyr::inner_join(demo %>%
                          dplyr::select(healthCode, externalId)) %>%
    dplyr::select(healthCode, externalId, everything())


# save to synapse
save_to_synapse(
    data = pd.severity,
    output_filename = ref$pd_severity$baseline_superusers$output_filename, 
    parent = ref$pd_severity$baseline_superusers$parent_id,
    name = ref$pd_severity$baseline_superusers$provenance$name,
    description = ref$pd_severity$baseline_superusers$provenance$description,
    used = (train_test_mapping %>% unlist() %>% unname()))




