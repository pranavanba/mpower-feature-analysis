############################################################################
# Purpose: Created a study burst schedule for all participants in mpower 2.0
#          based on the earliest record in the Health Data Summary Table(syn12492996).
#          Code is repurposed from Phil's code to measure study burst summary for at-home-pd
#          https://github.com/Sage-Bionetworks/at-home-pd/blob/master/study_burst_summary/study_burst_summary.R
#
#    NOTE: 
#         We will not be using the dayInStudy column as it is unreliable. 
#         A single dayInStudy will have multiple createdOn. I suspect this issue
#         is because dayInStudy is calculated using the uploadDate and not date derived from createdOn
#         Some example queries that show this are:
#         (Data from Nov 2022) "SELECT * FROM syn12492996 where healthCode = 'HrWfVtFnM0i7A47zdlFDhgqz' and dayInStudy = 742"
#         (Data from Feb 2021) "SELECT * FROM syn12492996 where healthCode = '0b5bf2ec-1931-4d01-904f-3f3e50c16f6a' and dayInStudy = 1"
# 
# 
#' This script produces a table with columns:
#' 
#' * healthCode (str)
#' * study_burst (str)
#' * study_burst_start_date (str)
#' * study_burst_end_date (str)
#' * days_completed (int)
#' * study_burst_successful (bool)
#' 
#' And stores this result to TABLE_OUTPUT (global var below).
#' 
#' This is more granular than the table produced by compliance_overview.R,
#' which summarizes study burst compliance at the study burst level (Y1,Q1, etc.)
# 
# 
# 
# Author: Meghasyam Tummalacherla, Phil Snyder
############################################################################
rm(list=ls())
gc()

##############
# Required libraries
##############
library(synapser)
library(tidyverse)

HEALTH_DATA_SUMMARY_TABLE <- "syn12492996"
TABLE_OUTPUT <- 'syn50547144'

read_syn_table <- function(syn_id) {
  q <- synapser::synTableQuery(paste("select * from", syn_id))
  table <- q$asDataFrame() %>% 
    dplyr::as_tibble() 
  return(table)
}

get_timezone_as_integer <- function(createdOnTimeZone) {
  # If there is no timezone information we make the conservative
  # (for a US user) estimate that the time zone is Pacific
  if (is.na(createdOnTimeZone)) {
    return(-8)
  } else {
    cotz_integer <- as.integer(as.integer(createdOnTimeZone) / 100)
    return(cotz_integer)
  }
}

fetch_mpower <- function() {
  mpower <- read_syn_table(HEALTH_DATA_SUMMARY_TABLE)
  mpower$createdOnTimeZoneInteger <- unlist(purrr::map(mpower$createdOnTimeZone,
                                                       get_timezone_as_integer))
  mpower <- mpower %>% 
    dplyr::mutate(createdOnLocalTime = createdOn + lubridate::hours(createdOnTimeZoneInteger)) 
  
  return(mpower)
}

build_study_burst_summary <- function(mpower) {
  #' We make the conservative (for a US user) assumption that the current day is
  #' relative to Pacific time.
  first_activity <- mpower %>% 
    dplyr::group_by(activity_guid = guid) %>%
    dplyr::summarize(first_activity = lubridate::as_date(min(createdOnLocalTime, na.rm = T)),
                     currentDayInStudy = as.integer(
                       lubridate::today(tz="America/Los_Angeles") - first_activity),
                     currentlyInStudyBurst = currentDayInStudy %% 90 < 20)
  
  study_burst_dates <- purrr::map2_dfr(
    first_activity$activity_guid, first_activity$first_activity, function(guid, first_activity) {
      dates <- purrr::map_dfr(0:8, function(i) {
        dplyr::tibble(dates_guid = guid,
                      study_burst_number = i,
                      study_burst_start_date = first_activity + i * lubridate::days(90),
                      study_burst_end_date = study_burst_start_date + lubridate::days(19))
      })                                      
      return(dates)
    })
  days_completed <- purrr::pmap_dfr(first_activity,
                                    function(activity_guid, first_activity, currentDayInStudy, currentlyInStudyBurst) {
                                      relevant_study_burst_dates <- study_burst_dates %>%
                                        dplyr::filter(dates_guid == activity_guid)
                                      days_completed <- purrr::pmap_dfr(relevant_study_burst_dates,
                                                                        function(dates_guid, study_burst_number, study_burst_start_date, study_burst_end_date) {
                                                                          relevant_mpower <- mpower %>%
                                                                            dplyr::filter(originalTable == "StudyBurst-v1",
                                                                                          guid == dates_guid,
                                                                                          createdOnLocalTime >= study_burst_start_date,
                                                                                          createdOnLocalTime <= study_burst_end_date)
                                                                          days_completed_this_burst <- n_distinct(relevant_mpower$dayInStudy)
                                                                          # If the participant has not yet finished this study burst, store NA for days completed
                                                                          if (days_completed_this_burst == 0 && study_burst_end_date >= lubridate::today()) {
                                                                            days_completed_this_burst = NA
                                                                          }
                                                                          dplyr::tibble(days_guid = dates_guid,
                                                                                        study_burst_number = study_burst_number,
                                                                                        days_completed = days_completed_this_burst,
                                                                                        study_burst_successful = days_completed >= 10)
                                                                        })
                                      return(days_completed)
                                    })
  study_burst_summary <- mpower %>% 
    dplyr::distinct(guid) %>% 
    dplyr::left_join(study_burst_dates, by = c("guid" = "dates_guid")) %>% 
    dplyr::left_join(days_completed, by = c("guid" = "days_guid", "study_burst_number")) %>% 
    dplyr::rename(study_burst = study_burst_number) %>% 
    dplyr::mutate(study_burst = as.character(study_burst),
                  study_burst_start_date = as.character(study_burst_start_date),
                  study_burst_end_date = as.character(study_burst_end_date),
                  guid_prefix = str_extract(guid, "^.{3}")) %>% 
    plyr::arrange(guid, study_burst) %>% 
    dplyr::select(guid, guid_prefix, study_burst, study_burst_start_date,
                  study_burst_end_date, days_completed, study_burst_successful)
  study_burst_summary$study_burst <- dplyr::recode(
    study_burst_summary$study_burst, "0" = "Y1,Q1", "1" = "Y1,Q2", "2" = "Y1,Q3",
    "3" = "Y1,Q4", "4" = "Y2,Q1", "5" = "Y2,Q2", "6" = "Y2,Q3",
    "7" = "Y2,Q4", "8" = "Y3,Q1")
  return(study_burst_summary)
}

store_to_synapse <- function(study_burst_summary) {
  q <- synTableQuery(paste("select * from", TABLE_OUTPUT))
  synDelete(q) # Remove preexisting rows
  t <- synTable(TABLE_OUTPUT, study_burst_summary)  
  synStore(t,  executed=list(
    paste0("https://github.com/itismeghasyam/mpower-feature-analysis/",
           "blob/master/analysis/mpower_study_burst_summary.R")))
}

main <- function() {
  synLogin(Sys.getenv("synapseUsername"), Sys.getenv("synapsePassword"))
  mpower <- fetch_mpower()
  
  # Convert guid to healthCode
  mpower <- mpower %>% 
    dplyr::filter(createdOn > '2014-01-01' ) %>% 
    dplyr::mutate(guid = healthCode) 
  # Phil's code uses guid (which is the externalId in some instances. 
  # We need it by healthCode, so we set guid = healthCode
  
  study_burst_summary <- build_study_burst_summary(mpower) %>% 
    dplyr::select(-guid_prefix) %>% 
    dplyr::rename(healthCode=guid) %>% # get back healthCode
    dplyr::filter(study_burst_start_date <= Sys.Date()) # we don't need future study bursts
  
  store_to_synapse(study_burst_summary)
}

main()

