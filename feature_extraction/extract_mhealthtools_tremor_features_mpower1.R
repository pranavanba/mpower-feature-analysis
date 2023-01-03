############################################################################
# Purpose: Extract Tremor features for mpower participants
# Author: Meghasyam Tummalacherla
############################################################################
rm(list=ls())
gc()

##############
# Required libraries
##############
library(tidyverse)
library(synapser)
library(doMC)
library(jsonlite)
library(parallel)
library(githubr) 
# devtools::install_github("brian-bot/githubr")
library(mpowertools)
# devtools::install_github("Sage-Bionetworks/mpowertools")


#############
# Required functions
##############
# Preprocess the tremor file into a format that the older mpowertools 
# pipeline accepts it as input
processTremorFile <- function(tremorJsonFileLocation){
  # Read the Json File and process it into mhealthtools format
  
  tremorData <-   tryCatch({
    tremor_data <- jsonlite::fromJSON(as.character(tremorJsonFileLocation))
    
    accel_data <- tremor_data %>%
      dplyr::filter(sensorType == 'accelerometer') %>% 
      dplyr::select(timestamp, x,y,z) %>% 
      dplyr::mutate(timestamp = timestamp - min(timestamp, na.rm = T))
    
    # %>% 
    #   dplyr::select(-timestamp)
    
    gyro_data <- tremor_data %>%
      dplyr::filter(sensorType == 'gyro') %>% 
      dplyr::select(timestamp, x,y,z) %>% 
      dplyr::mutate(timestamp = timestamp - min(timestamp, na.rm = T))
    # %>% 
    #   dplyr::select(-timestamp)
    
    tremor_data <- list(userAcceleration = accel_data,
                        rotationRate = gyro_data)
  }, error = function(err) {
    tremor_data <- NA
  })
}

## functions to extract tremor features for a single tremor record
getTremorFeatures <- function(dat, windowLen = 256, freqRange = c(1, 25), ovlp = 0.5) {
  # written for a processed tremor file from V2
  
  # If no data exists
  ftrs = data.frame(Window = NA, error = NA)
  if(is.na(dat)){ ftrs$error = 'No data'; return(ftrs) }
  
  # Get sampling rate for accelerometer
  samplingRateAccel = length(dat$userAcceleration$timestamp)/(max(dat$userAcceleration$timestamp, na.rm = T) - min(dat$userAcceleration$timestamp,na.rm = T))
  
  # Get sampling rate for gyroscope
  samplingRateGyro = length(dat$rotationRate$timestamp)/(max(dat$rotationRate$timestamp, na.rm = T) - min(dat$rotationRate$timestamp, na.rm = T))
  
  # Get accelerometer features
  ftrs.acc = getTremorFeatures.userAccel(dat, samplingRateAccel, windowLen = windowLen, freqRange = freqRange, ovlp = ovlp)
  
  # Get accelerometer features
  ftrs.gyro = getTremorFeatures.rotRate(dat, samplingRateGyro, windowLen = windowLen, freqRange = freqRange, ovlp = ovlp)
  
  # Return if processing is errored
  if(!is.na(ftrs.acc$error) || !is.na(ftrs.gyro$error)){
    return(list(accelerometer = ftrs.acc, gyroscope = ftrs.gyro) %>%
             data.table::rbindlist(use.names = TRUE, fill = T, idcol = 'sensor'))
  }
  
  # Combine all features
  ftrs = list(accelerometer = ftrs.acc, gyroscope = ftrs.gyro) %>%
    data.table::rbindlist(use.names = TRUE, fill = T, idcol = 'sensor') %>%
    dplyr::mutate(Window = as.character(Window)) 

  return(ftrs)
}

# Function to extract tremor features from user acceleration
getTremorFeatures.userAccel <- function(dat, samplingRate, windowLen = 256, freqRange = c(1, 25), ovlp = 0.5) {
  
  ftrs = data.frame(Window = NA, error = NA)
  
  # Get user acceleration
  userAccel = tryCatch({
    userAccel = dat$userAcceleration %>%
      as.data.frame()
    ind = order(userAccel$timestamp)
    userAccel = userAccel[ind, ] %>%
      tidyr::gather(axis, accel, -timestamp)
  }, error = function(e){ NA })
  if(all(is.na(userAccel))){ ftrs$error = 'userAccel extraction error'; return(ftrs) }
  
  # Detrend data
  userAccel = tryCatch({
    userAccel %>%
      plyr::ddply(.variables = 'axis', .fun = function(x){
        x$accel = loess(x$accel~x$timestamp)$residual
        return(x)
      })
  }, error = function(e){ NA })
  if(all(is.na(userAccel))){ ftrs$error = 'Detrend error'; return(ftrs) }
  
  # Band pass filter signal between freqRange
  userAccel = tryCatch({
    userAccel %>% 
      plyr::ddply(.variables = 'axis', .fun = function(x, windowLen, sl, freqRange){
        bandPassFilt = signal::fir1(windowLen-1, c(freqRange[1] * 2/sl, freqRange[2] * 2/sl),
                                    type="pass", 
                                    window = seewave::hamming.w(windowLen))
        x$accel = signal::filtfilt(bandPassFilt, x$accel)
        return(x)
      }, windowLen, samplingRate, freqRange)
  }, error = function(e){ NA })
  if(all(is.na(userAccel))){ ftrs$error = 'Band pass filter error'; return(ftrs) }
  
  # Filter signal between 1 and 9 sec
  userAccel = tryCatch({
    userAccel %>%
      dplyr::filter(timestamp >= 1, timestamp <= 9)
  }, error = function(e){ NA })
  if(all(is.na(userAccel))){ ftrs$error = 'Not enough time samples'; return(ftrs) }
  
  # Split user acceleration into windows
  userAccel  = userAccel %>%
    plyr::dlply(.variables = 'axis', .fun = function(accel, windowLen, ovlp){
      a = mpowertools:::windowSignal(accel$accel, windowLen = windowLen, ovlp = ovlp) 
    }, windowLen, ovlp)
  
  # Get user jerk
  userJerk = userAccel %>%
    lapply(function(accel, sl){
      apply(accel,2, diff)*sl
    }, samplingRate)
  
  # Get user velocity
  userVel = userAccel %>%
    lapply(function(accel, sl){
      apply(accel,2, diffinv)*sl
    }, samplingRate)
  
  # Get user displacement
  userDisp = userVel %>%
    lapply(function(accel, sl){
      apply(accel,2, diffinv)*sl
    }, samplingRate)
  
  # Get acf of user accel
  userACF = userAccel %>%
    lapply(function(accel, sl){
      apply(accel,2, function(x){acf(x, plot = F)$acf})
    }, samplingRate)
  
  # Get time and frequency domain features for angular velocity, acceleration, displacement, auto correlation of velocity
  ftrs = list(ua = userAccel, uj = userJerk, uv = userVel,  ud = userDisp, uaacf = userACF) %>%
    plyr::ldply(.fun = function(userAccel){
      plyr::ldply(userAccel, .fun = function(accel){
        list(apply(accel, 2, mpowertools:::getTimeDomainSummary, samplingRate) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window'),
             apply(accel, 2, mpowertools:::getFrequencyDomainSummary, samplingRate = samplingRate, npeaks = 3) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window'),
             apply(accel, 2, mpowertools:::getFrequencyDomainEnergy, samplingRate) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window')) %>%
          plyr::join_all(by = 'Window')
      }, .id = 'axis')
    }, .id = 'measurementType') %>%
    dplyr::mutate(error = NA)
  
  return(ftrs)
}

# Function to extract tremor features from user angular velocity from gyroscope
getTremorFeatures.rotRate <- function(dat, samplingRate, windowLen = 256, freqRange = c(1, 25), ovlp = 0.5) {
  
  ftrs = data.frame(Window = NA, error = NA)
  
  # Get user angular velocity from gyro data
  userAngVel = tryCatch({
    userAngVel = dat$rotationRate %>% 
      as.data.frame()
    ind = order(userAngVel$timestamp)
    userAngVel = userAngVel[ind, ] %>%
      tidyr::gather(axis, angvel, -timestamp)
  }, error = function(e){ NA })
  if(all(is.na(userAngVel))){ ftrs$error = 'userAccel rotation error'; return(ftrs) }
  
  # Detrend data
  userAngVel = tryCatch({
    userAngVel %>%
      plyr::ddply(.variables = 'axis', .fun = function(x){
        x$angvel = loess(x$angvel~x$timestamp)$residual
        x <- return(x)
      })
  }, error = function(e){ NA })
  if(all(is.na(userAngVel))){ ftrs$error = 'Detrend error'; return(ftrs) }
  
  # Band pass filter signal between freqRange
  userAngVel = tryCatch({
    userAngVel %>% 
      plyr::ddply(.variables = 'axis', .fun = function(x, windowLen, sl, freqRange){
        bandPassFilt = signal::fir1(windowLen-1, c(freqRange[1] * 2/sl, freqRange[2] * 2/sl),
                                    type="pass", 
                                    window = seewave::hamming.w(windowLen))
        x$angvel = signal::filtfilt(bandPassFilt, x$angvel)
        return(x)
      }, windowLen, samplingRate, freqRange)
  }, error = function(e){ NA })
  if(all(is.na(userAngVel))){ ftrs$error = 'Band pass filter error'; return(ftrs) }
  
  # Filter signal between 1 and 9 sec
  userAngVel = tryCatch({
    userAngVel %>%
      dplyr::filter(timestamp >= 1, timestamp <= 9)
  }, error = function(e){ NA })
  if(all(is.na(userAngVel))){ ftrs$error = 'Not enough time samples'; return(ftrs) }
  
  # Split user acceleration into windows
  userAngVel  = userAngVel %>%
    plyr::dlply(.variables = 'axis', .fun = function(angvel, windowLen, ovlp){
      a = mpowertools:::windowSignal(angvel$angvel, windowLen = windowLen, ovlp = ovlp) 
    }, windowLen, ovlp)
  
  # Get user angular acceleration
  userAngAcc = userAngVel %>%
    lapply(function(angvel, sl){
      apply(angvel, 2, diff)*sl
    }, samplingRate)
  
  # Get user angular displacement
  userAngDis = userAngVel %>%
    lapply(function(angvel, sl){
      apply(angvel,2, diffinv)*sl
    }, samplingRate)
  
  # Get user acf (ACF of user angular velocity)
  userACF = userAngVel %>%
    lapply(function(angvel, sl){
      apply(angvel,2, function(x){acf(x, plot = F)$acf})
    }, samplingRate)
  
  # Get time and frequency domain features for angular velocity, acceleration, displacement, auto correlation of velocity
  ftrs = list(uav = userAngVel, uaa = userAngAcc, uad = userAngDis,  uavacf = userACF) %>%
    plyr::ldply(.fun = function(userAccel){
      plyr::ldply(userAccel, .fun = function(accel){
        list(apply(accel, 2, mpowertools:::getTimeDomainSummary, samplingRate) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window'),
             apply(accel, 2, mpowertools:::getFrequencyDomainSummary, samplingRate = samplingRate, npeaks = 3) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window'),
             apply(accel, 2, mpowertools:::getFrequencyDomainEnergy, samplingRate) %>%
               data.table::rbindlist(use.names = T, fill = T, idcol = 'Window')) %>%
          plyr::join_all(by = 'Window')
      }, .id = 'axis')
    }, .id = 'measurementType') %>%
    dplyr::mutate(error = NA)
  
  return(ftrs)
}

## functions to parallize the feature extraction
featuresFromColumn <- function(dat,column,processingFunction, parallel = F){
  # Apply the processingFunction to each row of the column in the dataframe dat
  
  plyr::ddply(
    .data = dat,
    .variables = colnames(dat),
    .parallel = parallel,
    .fun = function(row) {
      return(processingFunction(row[column]))
    }
  ) 
}

extractTremorFeaturesFromFile <- function(tremorJsonFileLocation){
  
  ftrs <- tryCatch({
    dat <- processTremorFile(tremorJsonFileLocation)
    ftrsin <- getTremorFeatures(dat)
  }, error = function(e){
    NA
    # do error handling here
  })
  return(ftrs)
}


extractTremorFeatures <- function(dat_, column_, runParallel_){
  tremor_features <- featuresFromColumn(
    dat = dat_,
    column = column_,
    processingFunction = function(tremorJsonFileLocation){
      tremorFeat <- extractTremorFeaturesFromFile(tremorJsonFileLocation)
      return(tremorFeat)
    },
    parallel = runParallel_ 
  )
  return(tremor_features) 
}

changeMeasurementType <- function(x){
  if(as.character(x[['sensor']]) == 'accelerometer'){
    if(x[['measurementType']] == 'acceleration'){
      return('ua')
    }
    if(x[['measurementType']] == 'jerk'){
      return('uj')
    }
    if(x[['measurementType']] == 'velocity'){
      return('uv')
    }
    if(x[['measurementType']] == 'displacement'){
      return('ud')
    }
    if(x[['measurementType']] == 'acf'){
      return('uaacf')
    }
  }
  if(as.character(x[['sensor']]) == 'gyroscope'){
    if(x[['measurementType']] == 'acceleration'){
      return('uaa')
    }
    if(x[['measurementType']] == 'jerk'){
      return('uj')
    }
    if(x[['measurementType']] == 'velocity'){
      return('uav')
    }
    if(x[['measurementType']] == 'displacement'){
      return('uad')
    }
    if(x[['measurementType']] == 'acf'){
      return('uavacf')
    }
  }
}


#############
# Download Synapse Table, and select and download required columns, figure out filepath locations
#############
# login to Synapse
synapser::synLogin(Sys.getenv('synapseUsername'),Sys.getenv('synapsePassword'), rememberMe = TRUE)

# set system environment to UTC
Sys.setenv(TZ='GMT')

tremor.tbl.id = 'syn12977322' # Tremor-v3
# Select only those healthCodes from the mpower tremor table that are present in the age matched healthcodes
tremor.tbl.syn <- synapser::synTableQuery(paste0("SELECT * FROM ", tremor.tbl.id))
tremor.tbl <- tremor.tbl.syn$asDataFrame()

## Download required columns i,e the JSON files
columnsToDownload = c("left_motion.json",
                      "right_motion.json") 

# start_time = Sys.time()
tremor.json.loc = lapply(columnsToDownload, function(col.name){
  tbl.files = synapser::synDownloadTableColumns(tremor.tbl.syn, col.name) %>%
    lapply(function(x) data.frame(V1 = x)) %>% 
    data.table::rbindlist(idcol = col.name) %>% 
    plyr::rename(c('V1' = gsub('.json','.fileLocation', col.name)))
})
# stop_time = Sys.time()
# print(stop_time - start_time)

## Merge file locations to main table to create a meta table
tremor.tbl.meta = data.table::rbindlist(
  list(tremor.tbl %>%
         dplyr::left_join(do.call(cbind, tremor.json.loc[1]))),
  use.names = T, fill = T) %>%
  as.data.frame
tremor.tbl.meta = data.table::rbindlist(
  list(tremor.tbl.meta %>%
         dplyr::left_join(do.call(cbind, tremor.json.loc[2]))),
  use.names = T, fill = T) %>%
  as.data.frame

## Convert column format from factors to strings for the fileLocations
tremor.tbl.meta$left_motion.fileLocation <- as.character(
  tremor.tbl.meta$left_motion.fileLocation)
tremor.tbl.meta$right_motion.fileLocation <- as.character(
  tremor.tbl.meta$right_motion.fileLocation)

#############
# Extract Tremor features
##############
if (detectCores() >= 2) {
  runParallel <- TRUE
} else {
  runParallel <- FALSE
}
doMC::registerDoMC(detectCores() - 2)

# Left Hand Features
tremor.tbl.meta.noNA.left <- tremor.tbl.meta[!is.na(tremor.tbl.meta$left_motion.fileLocation),] %>%
  dplyr::select(recordId, left_motion.fileLocation)

tremor.tbl.meta.noNA <- tremor.tbl.meta.noNA.left
start_time = Sys.time()
tremor_features_left_1 <- extractTremorFeatures(
  dat_ = tremor.tbl.meta.noNA,
  column_ = "left_motion.fileLocation",
  runParallel_ = runParallel)
print(Sys.time() - start_time)
gc()

# Remove the error tremor data frames by filtering on skew.fr
# (look at errorTremorFeatureDataFrame)
tremor_features_left_1 <- tremor_features_left_1 %>%
  dplyr::filter(skew.fr != -88888) %>% 
  dplyr::select(-left_motion.fileLocation)

data.table::fwrite(tremor_features_left_1, 'tremor_features_left.tsv', sep = '\t')


## right Hand Features
tremor.tbl.meta.noNA.right <- tremor.tbl.meta[!is.na(tremor.tbl.meta$right_motion.fileLocation),] %>%
  dplyr::select(recordId, right_motion.fileLocation)

tremor.tbl.meta.noNA <- tremor.tbl.meta.noNA.right
tremor_features_right_1 <- extractTremorFeatures(
  dat_ = tremor.tbl.meta.noNA,
  column_ = "right_motion.fileLocation",
  runParallel_ = runParallel)
gc()
# 

# Remove the error tremor data frames by filtering on skew.fr
# (look at errorTremorFeatureDataFrame)
tremor_features_right_1 <- tremor_features_right_1 %>%
  dplyr::filter(skew.fr != -88888) %>% 
  dplyr::select(-right_motion.fileLocation)

data.table::fwrite(tremor_features_right_1, 'tremor_features_right.tsv', sep = '\t')



#############
# Upload data to Synapse
#############
# upload file to Synapse with provenance
# to learn more about provenance in Synapse, go to http://docs.synapse.org/articles/provenance.html

## Github link
# Copy paste the github token string and store it as 'github_token.txt' file
# A github token is required to access the elevateMS_analysis repository as it is private
gtToken = '~/github_token.txt'
githubr::setGithubToken(as.character(read.table(gtToken)$V1))
thisFileName <- "featureExtraction/extract_mhealthtools_tremor_features_mpower1.R" # location of file inside github repo
thisRepo <- getRepo(repository = "itismeghasyam/mpower-feature-analysis", 
                    ref="branch", 
                    refName="master")
thisFile <- getPermlink(repository = thisRepo, repositoryPath=thisFileName)
 
# upload to Synapse, left hand features
synapse.folder.id <- "syn50559334" # synId of folder to upload your file to
synStore(File('tremor_features_left.tsv', parentId=synapse.folder.id))

# upload to Synapse, right hand features
synapse.folder.id <- "syn50559334" # synId of folder to upload your file to
synStore(File('tremor_features_right.tsv', parentId=synapse.folder.id))
