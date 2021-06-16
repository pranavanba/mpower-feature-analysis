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


match_healthcodes  <- function(agg_features, 
                               records_thresh = 5,
                               plot = FALSE){
    cleaned_features <- agg_features %>% 
        dplyr::filter(
            nrecords >= records_thresh &
            diagnosis != "no_answer" &
            age > 0 & 
            age <= 120) %>%
        tidyr::drop_na(age, sex, diagnosis) %>%
        dplyr::mutate(PD = ifelse(diagnosis == "parkinsons", 1, 0))
    
    ## perform the matching
    matched_data <- age_gender_matching(cleaned_features) %>%
        dplyr::mutate(PD = ifelse(PD == 1, "parkinsons", "control"))
    
    if(plot == TRUE){
        before_matching <- cleaned_features %>% 
            ggplot(aes(x = diagnosis, y = age)) + 
            geom_boxplot() +
            labs(title = "Before Matching") +
            facet_wrap(~sex, scale="free")
        after_matching <- matched_data %>% 
            ggplot(aes(x = PD, y = age)) + 
            geom_boxplot() +
            labs(title = "After Matching") +
            facet_wrap(~sex, scale="free")
        before_after_plot <- patchwork::wrap_plots(before_matching, 
                                      after_matching,
                                      nrow = 2)
    }else{
        before_after_plot <- NULL
    }
    
    result <- list(
        data = matched_data,
        plot = before_after_plot)
    
    
    return(result)
}


get_two_sample_ttest <- function(feature_data, 
                                 features,
                                 group){
    formula <- as.formula(glue::glue("value ~ {group}"))
    feature_data %>%
        dplyr::select(healthCode, all_of(c(features, group))) %>% 
        tidyr::pivot_longer(cols = all_of(features),
                            names_to = "feature") %>%
        dplyr::group_by(feature) %>%
        tidyr::nest() %>%
        dplyr::mutate(test = map(data, ~ wilcox.test(formula, 
                                                     data = .x,
                                                     exact = FALSE)),
                      tidied = map(test, broom::tidy),
                      n = map(data, ~ length(unique(.x$healthCode)))) %>%
        unnest(cols = c(tidied,n)) %>% 
        dplyr::select(n_user = n,
                      feature,
                      method,
                      p.value) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(p.value)
}

plot_boxplot <- function(data, feature, group){
    data %>% 
        ggplot(aes_string(y = feature, 
                          x = group,
                          fill = group)) +
        geom_boxplot(alpha = 0.5) + 
        geom_jitter(
            alpha = 0.5,
            position=position_jitter(width=.1, height=0)) +
        ggpubr::stat_compare_means() + 
        scale_fill_brewer(palette = "Dark2") +
        theme_minimal()
}
