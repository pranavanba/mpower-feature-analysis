## Predict mPTS per record

## libraries
library(synapser)
library(data.table)
library(plyr)
library(tidyverse)
library(knitr)
library(githubr)
library(lubridate)
library(ranger)
library(ROCR)
library(Boruta)
library(MatchIt)
library(caret)
library(future)
library(furrr)
source("utils/curation_utils.R")
source("utils/helper_utils.R")
synapser::synLogin(Sys.getenv('synapseUsername'),Sys.getenv('synapsePassword'), rememberMe = TRUE)

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


## Required functions
## Random forest model perfomance evaluation 
rfModelPerformance <- function(rf.mdl, datTest){
  rf.prediction = predict(rf.mdl, 
                          data = datTest,
                          predict.all = T,
                          type = 'response',
                          seed = 123456)
  pred.probabilities = rowSums(rf.prediction$predictions == max(datTest$PD), 
                               na.rm = T)/rf.prediction$num.trees
  names(pred.probabilities) = rownames(datTest)
  
  if(length(unique(datTest$PD)) == 2){
    mdl.prediction = pred.probabilities %>%
      prediction(predictions = ., 
                 labels = datTest[,'PD'],
                 label.ordering = c(0,1))
    
    auroc = data.frame(metricName = 'auroc', 
                       value = performance(mdl.prediction, 'auc')@y.values[[1]])
    
    acc = data.frame(metricName = 'acc',
                     value = max(performance(mdl.prediction, 'acc')@y.values[[1]]))
    rec = performance(mdl.prediction, 'rec')@y.values[[1]]
    pre = performance(mdl.prediction, 'prec')@y.values[[1]]
    
    aupr = data.frame(metricName = 'aupr', 
                      value = caTools::trapz(rec[!is.nan(rec) & !is.nan(pre)], 
                                             pre[!is.nan(rec) & !is.nan(pre)]))
    mdl.performance = data.frame(auroc = auroc$value, 
                                 acc = acc$value, 
                                 aupr = aupr$value)
  } else {
    mdl.performance = data.frame(auroc = NA, acc = NA, aupr = NA)
  }
  
  return(list(pred.probabilities = data.frame(pred.probabilities) %>%
                rownames_to_column('recordId'),
              performance = mdl.performance))
  
}


## Download mpower 2.0 features and mpower 1.0 model from Synapse
# Get trained models(rest) from synaspe for mpower 1
pre.trained.models = c(tremor = 'syn13681030') %>%
  imap(.f = function(id, nid){
    load(synapser::synGet(id)$path)
    tmp = rfModel.ga %>%
      imap(.f = function(x, nx){
        tmp = x$trainedMdl %>%
          imap(.f = function(y, ny){
            y %>%
              map(.f = function(z){
                z$mdl
              }) %>%
              set_names(nm = paste(ny, names(y), sep = '.'))
          }) %>%
          flatten()
        names(tmp) = paste(nx, names(tmp), sep = '.')
        return(tmp)
      }) %>%
      flatten()
    names(tmp) = paste(nid, names(tmp), sep = '.')
    tmp = tmp[grep('alternateModel', names(tmp))]
    
    return(tmp)  
  })


## Get diganosis (PD) status
demo.tbl <- synapser::synTableQuery('SELECT * FROM syn15673379')$asDataFrame()
tremor.tbl <- synapser::synTableQuery('SELECT * FROM syn12977322')$asDataFrame()

pd.stats <- tremor.tbl %>% 
  dplyr::left_join(demo.tbl %>% dplyr::select(healthCode, diagnosis) %>% unique()) %>% 
  dplyr::select(recordId, healthCode, PD = diagnosis) %>% 
  unique()

# mpower 2 features
ftrs <- synapser::synGet('syn50559969')$path %>%
  data.table::fread() %>% 
  dplyr::left_join(pd.stats) %>% 
  tidyr::gather(feature, value, -recordId, -healthCode, -PD) %>%
  dplyr::filter(!is.na(value), value != '',
                PD %in% c('control', 'parkinsons')) %>%
  distinct() 

# There is an assay column called tremor that is character, remove it and set all value to numeric
ftrs <- ftrs %>% 
  dplyr::filter(!feature == 'assay') %>% 
  dplyr::mutate(value = as.numeric(value))


ftrs1 <- ftrs %>% 
  dplyr::filter(healthCode %in% ftrs$healthCode[1:10])
  
## Predict mPTS on mpower 2.0 features using the mpower 1.0 model
# Get recordwise performance/mPTS scores
rw.performance = pre.trained.models %>%
  imap(.f = function(mdl, nmdl, ftrs){
    print(nmdl)
    

    perf = ftrs %>%
      dplyr::mutate(studyName = 'otherData') %>%
      group_by(studyName, feature, healthCode) %>%
      mutate(value = scale(as.numeric(value)),
             PD = fct_recode(PD, '0' = 'control', '1' = 'parkinsons'),
             PD = as.numeric(PD) - 1) %>%
      ungroup() %>%
      group_by(studyName) %>%
      nest() %>%
      deframe() %>%
      na.omit()

    print('here')
    
    pp <- perf %>%
      map(.f = function(x){
        # print(x)
        
        datTest = x %>%
          dplyr::select(recordId, PD, feature, value) %>%
          tidyr::spread(feature, value) 
        
        
        ## Remove records with multiple entries -> There can be only one entry per record. 
        ## This needs to be checked why this is happening. For now we have 108 records out of 20855 that have multiple entries
        ## The reason is apparently the two rows corresponding to each record differ in PD status, 0 and 1 - ie., the records
        ## are being registered as both control and PD - this correction needs to happen upstream somewhere
        
        nrec_datTest <- datTest %>% 
          dplyr::group_by(recordId) %>% 
          dplyr::count() %>% 
          dplyr::filter(n > 1)
        
        datTest <- datTest %>% 
          dplyr::filter(!(recordId %in% nrec_datTest$recordId)) %>% 
          column_to_rownames(var = 'recordId')
        
        # rownames(datTest) <- datTest$recordId
        
        datTest[is.na(datTest)] = 0
        
        innerPerf = map(mdl,
                        .f = function(innerMdl, datTest){
                          rfModelPerformance(innerMdl, datTest)
                        }, datTest)
      }) 
  }, ftrs)

mPTS = rw.performance %>%
  map(.f = function(x){
    x %>%
      map(.f = function(y){
        y %>%
          map(.f = function(z){
            z$pred.probabilities
          }) %>%
          bind_rows(.id = 'idcol')
      }) %>%
      bind_rows(.id = 'studyName')
  }) %>%
  bind_rows() %>%
  tidyr::separate(idcol, c('assay', 'splitIteration', 'modelName', 'permIteration'), sep = '\\.') %>%
  dplyr::filter(modelName == 'alternateModel') %>%
  dplyr::group_by(assay, recordId) %>%
  dplyr::summarise(pred.probabilities = median(pred.probabilities, na.rm = T))

# rm(ftrs)
## Get Burst information per record (and healthCode), add to mPTS 
burst_info <- synapser::synTableQuery('SELECT * FROM syn50547144')$asDataFrame()

# get health data summary table
summary.tbl <- synapser::synTableQuery('SELECT * FROM syn12492996')$asDataFrame()

# subset to those tables we want, i.e tremor
all.burst.act.tbl <- summary.tbl %>% 
  dplyr::filter(originalTable %in% c('Tremor-v3'))

# burst info per record
all.tremor.act <- all.burst.act.tbl %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(burst = getBurstInfo(createdOn, healthCode, burst_info)) %>% 
  dplyr::ungroup()

## healthCodes with multiple bursts
tremor.hc.burst <- all.tremor.act %>% 
  dplyr::select(healthCode, burst) %>% 
  unique() %>% 
  na.omit %>% # remove non-burst data
  dplyr::group_by(healthCode) %>% 
  dplyr::mutate(nBurst = n()) %>% 
  ungroup()

## per record mPTS with burst information
mPTS_per_record <- mPTS %>% 
  dplyr::rename(mPTS = pred.probabilities) %>% 
  dplyr::left_join(all.tremor.act %>% 
                     dplyr::select(recordId, healthCode, burst)) %>% 
  unique()
  
# upload this to synapse
save_to_synapse(
  data = mPTS_per_record,
  output_filename = 'mPTS_per_record.tsv',
  parent = 'syn26947113',
  name = 'mPTS',
  description = 'mPTS based on pre built models from syn13681030')

