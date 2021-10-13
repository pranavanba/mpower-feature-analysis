tapping_features: tapping_features_v1 tapping_features_v2
tapping_features_v1:
	Rscript feature_extraction/tapping/extract_mhealthtools_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn10374665" \
	-o "mhealthtools_20secs_tapping_v1_features.tsv" \
	-n "run tapping feature extraction" \
	-f "tapping_results.json.TappingSamples, tapping_right.json.TappingSamples, tapping_left.json.TappingSamples" \
	-p "syn26250286" \
	-v 1;
tapping_features_v2:
	Rscript feature_extraction/tapping/extract_mhealthtools_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn15673381" \
	-p "syn26215075" \
	-o "mhealthtools_20secs_tapping_v2_features.tsv" \
	-n "run tapping feature extraction";
clean_tapping_features_v1:
	Rscript feature_processing/tapping/clean_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn10374665" \
	-f "syn26299031" \
	-p "syn26262474" \
	-o "cleaned_mhealthtools_20secs_filter_button_none_tapping_v1_freeze_features.tsv" \
	-n "clean tapping feature" \
	-m "recordId, createdOn, healthCode, appVersion, phoneInfo, dataGroups, medTimepoint" \
	-d "added metadata";
aggregate_tapping_features_v1:
	Rscript feature_processing/tapping/clean_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn10374665" \
	-f "syn26299031" \
	-p "syn26262474" \
	-o "agg_mhealthtools_20secs_filter_button_none_tapping_v1_freeze_features.tsv" \
	-n "clean tapping feature" \
	-d "added metadata, aggregate features" \
	-m "recordId, createdOn, healthCode, appVersion, phoneInfo, dataGroups, medTimepoint" \
	-a "healthCode";
clean_tapping_features_v2:
	Rscript feature_processing/tapping/clean_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn15673381" \
	-f "syn26301264" \
	-p "syn26262362" \
	-o "cleaned_mhealthtools_20secs_filter_button_none_tapping_v2_features.tsv" \
	-n "clean tapping feature" \
	-d "added metadata";
aggregate_tapping_features_v2:
	Rscript feature_processing/tapping/clean_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn15673381" \
	-f "syn26301264" \
	-p "syn26262362" \
	-o "agg_mhealthtools_20secs_filter_button_none_tapping_v2_features.tsv" \
	-n "clean tapping feature, aggregate features" \
	-d "added metadata" \
	-a "healthCode";

	