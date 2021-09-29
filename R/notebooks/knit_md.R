library(knit2synapse)
library(synapser)

knit2synapse::createAndKnitToFileEntity(
    file = "R/notebooks/walk_features_pca.rmd",
    parentId = "syn24610535",
    fileName = "PCA_walk_features",
    activity = synapser::Activity(
        name = "run PCA analysis",
        used = c("syn25782772",
                 "syn25421316",
                 "syn25782946"),
        executed = "https://github.com/arytontediarjo/mpower-feature-analysis/blob/master/R/notebooks/walk_features_pca.Rmd")
)
