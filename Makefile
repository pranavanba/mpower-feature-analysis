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
	-f "syn26344657" \
	-p "syn26262474" \
	-o "cleaned_mhealthtools_20secs_tapping_v1_freeze_features.tsv" \
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
	-f "syn26235452" \
	-p "syn26262362" \
	-o "cleaned_mhealthtools_20secs_tapping_v2_features.tsv" \
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
walk_features_v1:
	Rscript feature_extraction/walk30secs/extract_pdkit_rotation_walk30secs_features.R \
	-g "~/git_token.txt" \
	-i "syn10308918" \
	-o "pdkit_rotation_walk30secs_freeze_features.tsv" \
	-n "run walk feature extraction" \
	-f "accel_walking_outbound.json.items, deviceMotion_walking_outbound.json.items, accel_walking_return.json.items, deviceMotion_walking_return.json.items" \
	-p "syn26434895" \
	-v 1;
clean_walk_features_v1:
	Rscript feature_processing/walk30secs/clean_walk30secs_features.R \
	-g "~/git_token.txt" \
	-i "syn10308918" \
	-o "cleaned_pdkit_rotation_walk30secs_freeze_features.tsv" \
	-n "run walk feature cleaning" \
	-f "syn26434898" \
	-m "recordId, createdOn, healthCode, appVersion, phoneInfo, dataGroups, medTimepoint" \
	-p "syn26449904";
walk_features_v2:
	Rscript feature_extraction/walk30secs/extract_pdkit_rotation_walk30secs_features.R \
	-g "~/git_token.txt" \
	-i "syn26459524" \
	-o "pdkit_rotation_walk30secs_features_udall.tsv" \
	-n "run walk feature extraction" \
	-f "walk_motion.json" \
	-p "syn26467789" \
	-v 2;
clean_walk_features_v2:
	Rscript feature_processing/walk30secs/clean_walk30secs_features.R \
	-g "~/git_token.txt" \
	-i "syn12514611" \
	-o "cleaned_pdkit_rotation_walk30secs_v2_features.tsv" \
	-n "run walk feature cleaning" \
	-f "syn26434900" \
	-p "syn26341967";
superusers_tapping:
	Rscript feature_processing/superusers/baseline_activity.R \
	-o "mhealthtools_tapping_superusers_baseline.tsv" \
	-f "syn26344786" \
	-p "syn26142249" \
	-i "syn15673381";
superusers_walking:
	Rscript feature_processing/superusers/baseline_activity.R \
	-o "pdkit_walk30secs_superusers_baseline.tsv" \
	-p "syn26142249" \
	-f "syn26449887" \
	-i "syn12514611";
superusers_tremor:
	Rscript feature_processing/superusers/baseline_activity.R \
	-o "mhealthtools_tremor_superusers_baseline.tsv" \
	-p "syn26142249" \
	-f "syn26215339" \
	-i "syn12977322";
	
