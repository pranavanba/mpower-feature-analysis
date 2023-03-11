library(randomForest)
library(data.table)
library(synapser)
library(githubr)
library(tidyverse)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
synapser::synLogin()

## Required functions
getBurstInfo <- function(createdOn, healthCodeIn, ref.dat){
    ref.dat <- ref.dat %>% 
        dplyr::filter(healthCode == healthCodeIn)
    
    if(nrow(ref.dat) == 0){
        return(NA)
    }else{
      ref.dat <- ref.dat %>% 
          dplyr::mutate(in_burst =  (createdOn >= study_burst_start_date) & (createdOn <= study_burst_end_date)) %>% 
          dplyr::filter(in_burst) 
      return(ref.dat$study_burst[1])
    }
}

# load matched HC
load(synGet("syn15355694")$path)

# demographics of superPD users
demo_id <- "syn26840734"

# get the burst informaton
burst_info <- synapser::synTableQuery('SELECT * FROM syn50547144')$asDataFrame()

# get health data summary table
summary.tbl <- synapser::synTableQuery('SELECT * FROM syn12492996')$asDataFrame()

# subset to those tables we want, i.e tap,walk,tremor for now
all.burst.act.tbl <- summary.tbl %>% 
  dplyr::filter(originalTable %in% c('Tapping-v4','Tremor-v3','WalkAndBalance-v1'))

# multiple tap burst
all.tap.act <- all.burst.act.tbl %>% 
  dplyr::filter(originalTable == 'Tapping-v4') %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(burst = getBurstInfo(createdOn, healthCode, burst_info)) %>% 
  dplyr::ungroup()
  

## healthCodes with multiple bursts
tap.hc.burst <- all.tap.act %>% 
  dplyr::select(healthCode, burst) %>% 
  unique() %>% 
  na.omit %>% # remove non-burst data
  dplyr::group_by(healthCode) %>% 
  dplyr::mutate(nBurst = n()) %>% 
  ungroup()

# train-test mapping
train_test_mapping <- list(
    tapping = list(train = "syn25782775", 
                   test = "syn26947315"), 
    walking = list(train = "syn25782772", 
                   test = "syn26947548")
)

# output reference
OUTPUT_REF <- list(
    filename = "mPower_PD_conf_score_burst.tsv",
    parent = "syn26947113",
    name = "Random Forest Model",
    description = "Random Forest based on tapping and walking",
    annotations = list(
        analysisType = "PD severity",
        pipelineStep = "prediction"
    ),
    used = train_test_mapping %>% 
        unlist(recursive = T) %>% 
        unname()
)


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
                        header = TRUE, quote = "", stringsAsFactors = F)
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
  dplyr::left_join(summary.tbl %>% 
                     dplyr::select(healthCode, recordId, createdOn)) %>% 
    dplyr::select(recordId, createdOn, healthCode, all_of(tap.features)) %>%
    as.data.frame(.)

## get burst information as per recordId and createdOnDate
tap.test <- tap.test %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(burst = getBurstInfo(createdOn, healthCode, burst_info)) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(healthCodeBurst = paste0(healthCode, '||', burst)) %>% 
    dplyr::mutate(healthCodeOrig = healthCode) %>% 
    dplyr::mutate(healthCode = healthCodeBurst)

## Get number of records per burst for walk, tap etc.,
tap.burst.metrics <- tap.test %>% 
  dplyr::select(healthCode, recordId, burst) %>% 
  unique() %>% 
  # dplyr::filter(!is.na(burst)) %>% 
  dplyr::group_by(healthCode, burst) %>% 
  dplyr::count() %>% 
  dplyr::rename(nTapRecords = n) %>% 
  dplyr::ungroup() %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(burst = ifelse(is.na(burst),'NA',burst)) %>% 
  dplyr::ungroup()


tap.test <- GetCollapsedFeaturesmPower2(
    dat = tap.test, featNames = tap.features)
feature.names.tap <- intersect(names(tap.train), names(tap.test))
feature.names.tap <- setdiff(feature.names.tap,'healthCode')
pred.tap.rf <- GetRfPrediction(dat.train = tap.train, 
                               dat.test = tap.test, 
                               label.name = "diagnosis", 
                               feature.names = feature.names.tap,
                               pos.class.name = "parkinsons") %>%
    tibble::enframe() %>%
    dplyr::select(healthCode = name, tap = value)

pred.tap.rf <- pred.tap.rf %>% 
  dplyr::left_join(tap.burst.metrics) %>% 
  dplyr::rename(healthCodeMix = healthCode) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(healthCode = strsplit(healthCodeMix,'\\|\\|')[[1]][1]) %>% 
  dplyr::mutate(burst = strsplit(healthCodeMix,'\\|\\|')[[1]][2]) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(-healthCodeMix) 


# %>% 
#   dplyr::filter(burst != 'NA') # remove non-burst data
  

write.csv(pred.tap.rf, 'tapping_pred_burst.csv')


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
    as.data.frame(.)

wal.tbl <- synapser::synTableQuery('SELECT * FROM syn12514611')$asDataFrame()

## get burst information as per recordId and createdOnDate
wal.test <- wal.test %>% 
  dplyr::left_join(wal.tbl %>% 
                     dplyr::select(recordId,healthCode, createdOn)) %>% 
  dplyr::rowwise() %>%
  dplyr::mutate(burst = getBurstInfo(createdOn, healthCode, burst_info)) %>% 
  dplyr::ungroup() %>%
  dplyr::mutate(healthCodeBurst = paste0(healthCode, '||', burst)) %>% 
  dplyr::mutate(healthCodeOrig = healthCode) %>% 
  dplyr::mutate(healthCode = healthCodeBurst)

## Get number of records per burst for walk, tap etc.,
wal.burst.metrics <- wal.test %>% 
  dplyr::select(healthCode, recordId, burst) %>% 
  unique() %>% 
  # dplyr::filter(!is.na(burst)) %>% 
  dplyr::group_by(healthCode, burst) %>% 
  dplyr::count() %>% 
  dplyr::rename(nWalRecords = n) %>% 
  dplyr::ungroup() %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(burst = ifelse(is.na(burst),'NA',burst)) %>% 
  dplyr::ungroup()


wal.test <- GetCollapsedFeaturesmPower2(
    dat = wal.test, 
    featNames = wal.features)
feature.names.wal <- intersect(names(wal.train), names(wal.test))
feature.names.wal <- setdiff(feature.names.wal,'healthCode')
pred.wal.rf <- GetRfPrediction(dat.train = wal.train, 
                               dat.test = wal.test, 
                               label.name = "diagnosis", 
                               feature.names = feature.names.wal,
                               pos.class.name = "parkinsons") %>%
    tibble::enframe() %>%
    dplyr::select(healthCode = name, walk = value)

pred.wal.rf <- pred.wal.rf %>% 
  dplyr::left_join(wal.burst.metrics) %>% 
  dplyr::rename(healthCodeMix = healthCode) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(healthCode = strsplit(healthCodeMix,'\\|\\|')[[1]][1]) %>% 
  dplyr::mutate(burst = strsplit(healthCodeMix,'\\|\\|')[[1]][2]) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(-healthCodeMix) 

# %>% 
#   na.omit() # remove non-burst data

write.csv(pred.wal.rf, 'walking_pred_burst.csv')

###########################################
#' Tremor severity scores (mPTS)
###########################################
mPTS <- synapser::synGet('syn51189982')$path %>% 
  read.csv(sep = '\t', stringsAsFactors = F) %>% 
  dplyr::group_by(healthCode, burst) %>% 
  dplyr::mutate(nTremorRecords = n()) %>% 
  dplyr::ungroup() %>% 
  dplyr::group_by(healthCode, burst, nTremorRecords) %>% 
  dplyr::summarise(mPTS = median(mPTS)) %>% 
  dplyr::ungroup() %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(burst = ifelse(is.na(burst),'NA',burst)) %>% 
  dplyr::ungroup()


###########################################
#' Merge results
###########################################
demo <- fread(synGet(demo_id)$path)
pred_list <- list(pred.tap.rf, pred.wal.rf, mPTS)
pd.severity <- pred_list %>%
    purrr::reduce(dplyr::full_join) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(mean_conf_score = mean(c(tap,`walk`, mPTS), na.rm = T)) %>%
    dplyr::mutate(burst = ifelse(burst == 'NA','out_of_burst',burst)) %>% 
    dplyr::ungroup() %>%
    # dplyr::inner_join(demo %>%
    #                       dplyr::select(healthCode, externalId)) %>%
    dplyr::select(healthCode, everything()) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(has_all_three = ifelse(!(is.na(tap)||is.na(walk)||is.na(mPTS)), TRUE, FALSE),
                has_atleast_two = (((!is.na(tap))+(!is.na(walk))+(!is.na(mPTS))) >1)) %>% 
  dplyr::ungroup()


# save to synapse
save_to_synapse(
    data = pd.severity,
    output_filename = OUTPUT_REF$filename,
    parent = OUTPUT_REF$parent,
    annotations = OUTPUT_REF$annotations,
    name = OUTPUT_REF$name,
    description = OUTPUT_REF$description,
    used = OUTPUT_REF$used)

