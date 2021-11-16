## package dependencies
library(ranger)
library(caret)
library(data.table)
library(tidyverse)
library(patchwork)
library(ggplot2)
library(synapser)
source("utils/curation_utils.R")


synaper::synLogin()
FEATURES <- list(
    v1 = "syn26344790",
    v2 = "syn26344786"
)

########################################
## utility functions for data munging
########################################

## Collapse the data
##
## INPUTS
## dat: data.frame with multiple records per subject
## subject.id.name: name of the column with subject id codes 
##                  (e.g., "healthCode")
## attribute.name: name of the column of the additional attribute
##                 (e.g., "version" or "phoneInfo")
## feature.names: names of the features to be collapsed
##
## OUTPUT
## cdat: data.frame with collapsed features (the first 2 columns
##       are given by the "healthCode" and the "attribute")
##
CollapseFeaturesBySubjectIdAndAttribute <- function(dat, 
                                                    subject.id.name,
                                                    attribute.name,
                                                    feature.names) {
    ## Collapse the data at the subject id level
    ##
    ## INPUTs and OUTPUT as before
    ## 
    CollapseFeaturesBySubjectId <- function(dat,
                                            subject.id.name,
                                            attribute.name,
                                            feature.names) {
        ids <- na.omit(as.character(unique(dat[, subject.id.name]))) 
        dat[, attribute.name] <- as.character(dat[, attribute.name])
        nids <- length(ids)
        nfeat <- length(feature.names)
        out <- data.frame(matrix(NA, nids, 2*nfeat + 2))
        colnames(out) <- c(subject.id.name, 
                           attribute.name,
                           paste(feature.names, "md", sep = "."), 
                           paste(feature.names, "iqr", sep = "."))
        rownames(out) <- ids
        for (i in seq(nids)) {
            sdat <- dat[which(dat[, subject.id.name] == ids[i]),]
            out[i, subject.id.name] <- ids[i]
            ## because we are calling the "CollapseFeaturesBySubjectId" 
            ## function inside a inner loop where we first subset the 
            ## data by the attribute leve it follows that we can safely
            ## grab the firts value as all values are constant in this
            ## column
            out[i, attribute.name] <- sdat[1, attribute.name] 
            out[i, 3:(nfeat+2)] <- apply(sdat[, feature.names], 2, median, na.rm = TRUE)
            out[i, (nfeat+3):(2*nfeat+2)] <- apply(sdat[, feature.names], 2, IQR, na.rm = TRUE)
        }
        out
    }
    attribute.levels <- as.character(unique(dat[, attribute.name]))
    n.attr.levels <- length(attribute.levels)
    aux <- vector(mode = "list", length = n.attr.levels)
    for (i in seq(n.attr.levels)) {
        cat(i, "\n")
        ## get the subset of the data for a single level of the attribute
        ## (e.g., "iPhone 10") and then collapse the data for each 
        ## healthCode in this subset
        adat <- dat[dat[, attribute.name] == attribute.levels[i],]
        aux[[i]] <- CollapseFeaturesBySubjectId(adat, 
                                                subject.id.name, 
                                                attribute.name, 
                                                feature.names)
    }
    ## catenate the collapsed data.frames
    cdat <- aux[[1]]
    for (i in c(2:n.attr.levels)) {
        cdat <- rbind(cdat, aux[[i]])
    }
    
    cdat
}


####################################################
## utility functions for multiclass classification
####################################################

## Split the data into training and test sets in a
## "subject-wise" manner (so that all records of a
##  subject are either in the training or in the 
##  test set)
##
## INPUTS
## dat: data.frame (of collapsed features)
## subject.id.name: name of the column with subject id codes 
##                  (e.g., "healthCode")
## train.prop: proportion of the data assigned to training set
##             (train.prop = 0.50 assigns 50% of the healthCodes to training,
##              train.prop = 0.75 assigns 75% of the healthCodes to training)
## 
## OUTPUT
## idx.train: indexes for the training data
## idx.test: indexes for the test data
##
SubjectwiseSplit <- function(dat,
                             subject.id.name,
                             train.prop = 0.5) {
    ids <- as.character(unique(dat[, subject.id.name]))
    n.ids <- length(ids)
    train.ids <- sample(ids, round(n.ids * train.prop), replace = FALSE)
    test.ids <- setdiff(ids, train.ids)
    idx.train <- which(dat[, subject.id.name] %in% train.ids)
    idx.test <- which(dat[, subject.id.name] %in% test.ids)
    
    list(idx.train = idx.train, idx.test = idx.test)
}


## Extracts performance statistics for the multi-class classifier
## 
## INPUTS
## y.test: test set labels
## pred.probs: matrix of predicted class probabilities
##
## OUTPUTS
## tb: confusion matrix table
## acc: multiclass accuracy
##
MulticlassStats <- function(y.test, pred.probs) {
    label.classes <- colnames(pred.probs)
    y.hat <- label.classes[apply(pred.probs, 1, which.max)]
    aux <- caret::confusionMatrix(data = factor(y.hat), reference = factor(y.test))
    tb <- aux$table
    acc <- as.numeric(aux$overall["Accuracy"])
    
    list(tb = tb, acc = acc)
}


## Run random forest multi-class classifier
##
## INPUTS 
## n.runs: number of train/test data splits
## other parameters: as described before
##
## OUTPUTS
## acc: multiclass accuracy (vector of length n.runs)
## acc.perm: multiclass accuracy on permuted data (vector of length n.runs)
## cm: confusion (array of length n.runs)
## cm.perm: confusion on permuted data (array of length n.runs)
## Imp: random forest importance (matrix n.runs by number of features)
##
RangerMultiClass <- function(n.runs, 
                             dat, 
                             label.name, 
                             feature.names,
                             subject.id.name,
                             train.prop) {
    
    dat <- dat[, c(subject.id.name, label.name, feature.names)]
    dat <- na.omit(dat)
    n <- nrow(dat)
    label.column <- match(label.name, names(dat))
    feature.columns <- match(feature.names, names(dat))
    
    acc <- rep(NA, n.runs)
    acc.perm <- rep(NA, n.runs)
    cm <- vector(mode = "list", length = n.runs)
    cm.perm <- vector(mode = "list", length = n.runs)
    Imp <- matrix(NA, n.runs, length(feature.names))
    colnames(Imp) <- feature.names
    
    for (i in seq(n.runs)) {
        cat(i, "\n")
        
        ## original data
        aux.split <- SubjectwiseSplit(dat, subject.id.name, train.prop)
        dat.train <- dat[aux.split$idx.train,]
        dat.test <- dat[aux.split$idx.test,]
        my.formula <- as.formula(paste0(label.name, " ~ ", paste(feature.names, collapse = " + ")))
        fit <- ranger(my.formula, 
                      data = dat.train, 
                      probability = TRUE, 
                      verbose = FALSE,
                      importance = "impurity")
        pred.probs <- predict(fit, dat.test[, feature.columns, drop = FALSE], type = "response")$predictions
        y.test <- dat.test[, label.column]
        aux <- MulticlassStats(y.test, pred.probs)
        acc[i] <- aux$acc
        cm[[i]] <- aux$tb
        Imp[i,] <- aux$imp <- fit$variable.importance
        
        ## shuffled labels
        dat.perm <- dat
        dat.perm[, label.name] <- dat.perm[sample(n), label.name]
        
        aux.split <- SubjectwiseSplit(dat.perm, subject.id.name, train.prop)
        dat.train <- dat.perm[aux.split$idx.train,]
        dat.test <- dat.perm[aux.split$idx.test,]
        my.formula <- as.formula(paste0(label.name, " ~ ", paste(feature.names, collapse = " + ")))
        fit <- ranger(my.formula, 
                      data = dat.train, 
                      probability = TRUE, 
                      verbose = FALSE,
                      importance = "impurity")
        pred.probs <- predict(fit, dat.test[, feature.columns, drop = FALSE], type = "response")$predictions
        y.test <- dat.test[, label.column]
        aux <- MulticlassStats(y.test, pred.probs)
        acc.perm[i] <- aux$acc
        cm.perm[[i]] <- aux$tb
    }
    
    list(acc = acc, acc.perm = acc.perm, cm = cm, cm.perm = cm.perm, Imp = Imp)
}


ImportancePlot <- function(x, fig.mar = c(5, 10, 4, 2)) {
    aux <- apply(x, 2, median)
    o <- order(aux)
    y <- x[, o]
    par(mar = fig.mar)
    boxplot(y, horizontal = TRUE, las = 2, xlab = "importance", cex = 0.5)
    par(mar = c(5, 4, 4, 2) + 0.1)
}

## Plot feature importances from the random forest fit
## ordered from highest to lowest importance (according 
## to the median value across all the train/test data
## splits)
##
## INPUTS
## x: matrix of importances generated by the RangerMultiClass function
## fig.mar: margins for the plot
##
## OUTPUT
## Ordered importance plot
##
ImportancePlot2 <- function(x) {
    aux <- x %>% 
        tibble::as_tibble() %>% 
        pivot_longer(cols = matches("iqr|md"))
    
    feature_rank <- aux %>% 
        dplyr::group_by(name) %>% 
        dplyr::summarise(median = median(value,na.rm = T)) %>% 
        dplyr::ungroup() %>%
        dplyr::arrange(median) %>%
        dplyr::slice(1:10) %>%
        .$name
    
    aux %>% 
        dplyr::filter(name %in% feature_rank) %>%
        dplyr::mutate(name = factor(name, levels = feature_rank)) %>% 
        ggplot(aes(y = name, x = value)) +
        geom_boxplot() +
        theme_minimal() +
        labs(y = "", 
             title = "Feature Importances",
             subtitle = "Ranked by Median Impurity")
}


#########################################################
## utility functions for univariate analyses (ks-tests)
#########################################################

## Compute ks-tests comparing each level of the attribute against
## each other for all features
##
## INPUT
## dat: data.frame (of collapsed features)
## feature.names: names of the features to be collapsed
## attribute.name: name of the column of the attribute
##
## OUTPUT
## ks.pvals: matrix with the (raw) p-values from the
##           ks-test (rows indexes the features, columns
##           indexes the 2-by-2 combinations of the 
##           attribute levels)
##
RunKSTests <- function(dat,
                       feature.names,
                       attribute.name) {
    attribute.levels <- sort(as.character(unique(dat[, attribute.name])))
    ## gets all 2-by-2 possible combinations of the attribute levels
    aux <- combn(attribute.levels, 2) 
    n.comb <- ncol(aux)
    n.feat <- length(feature.names)
    ks.pvals <- matrix(NA, n.feat, n.comb)
    rownames(ks.pvals) <- feature.names
    colnames(ks.pvals) <- apply(aux, 2, function(x) paste(x, collapse = "_vs_"))
    for (i in seq(n.feat)) {
        for (j in seq(n.comb)) {
            idx.1 <- which(dat[, attribute.name] == aux[1, j])
            idx.2 <- which(dat[, attribute.name] == aux[2, j])   
            ks.pvals[i, j] <- ks.test(dat[idx.1, feature.names[i]], dat[idx.2, feature.names[i]])$p.value
        }
    }
    
    ks.pvals
}

get_feature_diagnostics <- function(data, attribute){
    tap.2 <- data %>% 
        dplyr::select(healthCode, 
                      !!sym(attribute),
                      matches("Inter|Drift|Taps|XY")) %>%
    
        as.data.frame()
    
    feature_set <- tap.2 %>%
        dplyr::select(matches(
            "Inter|Drift|Taps|XY")) %>%
        names()
    
    ## collapse the data by healthCode inside each phone type
    ctap.2 <- CollapseFeaturesBySubjectIdAndAttribute(
        dat = tap.2, 
        subject.id.name = "healthCode",
        attribute.name = attribute,
        feature.names = feature_set) %>% 
        dplyr::mutate(!!sym(attribute) := as.factor(!!sym(attribute)))
    
    ## get summarized feature list
    summ_feature_list <- ctap.2 %>% 
        dplyr::select(matches("iqr|median")) %>% 
        names()
    
    ## train the random forest multiclass classifier
    o2 <- RangerMultiClass(n.runs = 100, 
                           dat = ctap.2, 
                           label.name = attribute, 
                           feature.names = summ_feature_list,
                           subject.id.name = "healthCode",
                           train.prop = 0.5)
    ks.all <- RunKSTests(dat = ctap.2,
                         feature.names = summ_feature_list,
                         attribute.name = attribute)
    
    return(list(model = o2, 
                ks_test = ks.all))
}

get_plots <- function(output_list){
    o2 <- output_list$model
    
    ## plot the multiclass accuracy
    out <- data.frame(o2$acc, o2$acc.perm)
    names(out) <- c("original", "shuffled")
    pred_boxplot <- out %>% 
        pivot_longer(cols = c("original", "shuffled")) %>% 
        ggplot(aes(x = name, y = value, fill = name)) +
        geom_boxplot(alpha = 0.7) +
        geom_point(position=position_jitterdodge(), alpha = 0.2) +
        labs(x = "", 
             y = "",
             title = "Multiclass-Accuracy") +
        theme_minimal() +
        theme(legend.position = "none")
    
    ## plot confusion matrix
    median_loc <- o2$acc %>%
        sample_median_loc()
    conf_mat <-  o2$cm[[median_loc]] %>%
        generate_conf_mat_plot()
    
    ## plot the feature importances
    importance <- ImportancePlot2(x = o2$Imp)
    
    ## ks_test
    ks_test <- output_list$ks_test %>% 
        tibble::as_tibble() %>%
        tidyr::pivot_longer(cols = all_of(names(.))) %>%
        dplyr::group_by(name) %>%
        dplyr::mutate(window_median = median(value)) %>%
        dplyr::mutate(value = -log10(value)) %>%
        dplyr::filter(-log10(window_median) < -log10(0.05)) %>%
        dplyr::ungroup() %>%
        ggplot(aes(y = reorder(name, desc(window_median)), x = value)) +
        geom_vline(xintercept = -log10(0.05), 
                   linetype = "twodash", color = "red") +
        geom_boxplot() +
        labs(x = "-log10(p)", 
             y = "",
             title = "KS-Test on Each Group Comparison",
             subtitle = "Ranked by Median P-Value") +
        theme_minimal()
    
   
    return(list(box = pred_boxplot,
                conf_mat = conf_mat,
                importance = importance,
                ks_test = ks_test))
}


get_simulation_data <- function(features){
    feature_list <- list()
    
    # Simulation 0:
    feature_list$no_filter <- tap_features
    
    # Simulation 1 (filter based on timestamps):
    feature_list$filter_timestamps <-  tap_features %>%
        dplyr::filter(end_timestamp > 19)
    
    # Simulation 2 (filter based on tap interval ranges):
    feature_list$filter_tap_inter <-  tap_features %>%
        dplyr::filter(maxTapInter < 2)
    
    # Simulation 3 (filter based on number taps):
    feature_list$filter_tap_ntaps <-  tap_features %>%
        dplyr::filter(numberTaps > 50, numberTaps < 400)
    
    # Simulation 4 (filter based on early versions):
    feature_list$filter_early_versions <-  tap_features %>%
        dplyr::filter(!stringr::str_detect(version, "version 1.0"),
                      !stringr::str_detect(version, "version 1.2"),
                      !stringr::str_detect(version, "version 1.1"))
    
    # Simulation 5 (filter all):
    feature_list$filter_all <-  tap_features %>%
        dplyr::filter(end_timestamp > 19,
                      numberTaps > 50,
                      numberTaps < 400,
                      maxTapInter < 2,
                      !stringr::str_detect(version, "version 1.0"),
                      !stringr::str_detect(version, "version 1.2"),
                      !stringr::str_detect(version, "version 1.1"))
    return(feature_list)
}

sample_median_loc <- function(vector){
    set.seed(100)
    which((vector < median(vector) + 0.1) & 
              (vector > median(vector) - 0.1)) %>%
        sample(1)
}

generate_conf_mat_plot <- function(conf_mat){
    conf_mat %>% 
        tibble::as_tibble() %>%
        ggplot(aes(x = Reference, 
                   y = Prediction, 
                   fill = n)) +
        geom_tile(color = "white") +
        geom_text(aes(label = n), 
                  color = "white", 
                  size = 4) +
        coord_fixed()
}



tap_features_v1 <- synGet(FEATURES$v1)$path %>% 
    fread() %>%
    dplyr::mutate(version_abbv = "V1")
tap_features_v2 <- synGet(FEATURES$v2)$path %>% 
    fread() %>%
    dplyr::mutate(version_abbv = "V2")
tap_features <- dplyr::bind_rows(
    tap_features_v1,
    tap_features_v2) %>%
    dplyr::filter(!stringr::str_detect(phoneInfo, "iPad"),
                  !stringr::str_detect(phoneInfo, "Simulator")) %>%
    curate_version_group()
    

tap_features_sim <-  tap_features %>%
    get_simulation_data()

version_diagnostics <- tap_features_sim %>%
    purrr::map(function(data){
        data %>%
            get_feature_diagnostics(attribute = "version_group")
    })

phoneInfo_diagnostics <- tap_features_sim %>%
    purrr::map(function(data){
        data %>%
            get_feature_diagnostics(attribute = "phoneInfo")
    })