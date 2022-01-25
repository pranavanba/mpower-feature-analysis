features:
	Rscript feature_extraction/tremor/extract_mhealthtools_tremor_features.R || exit 1
	Rscript feature_extraction/tapping/extract_mhealthtools_tapping_features.R || exit 1
	Rscript feature_extraction/walk30secs/extract_pdkit_rotation_walk30secs_features.R || exit
