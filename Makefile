
tapping_features: tapping_features_v1 tapping_features_v2
tapping_features_v1:
	Rscript feature_extraction/tapping/extract_mhealthtools_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn10374665" \
	-o "mhealthtools_20secs_clean_button_none_tapping_v1_features.tsv" \
	-n "run tapping feature extraction" \
	-f "tapping_results.json.TappingSamples, tapping_right.json.TappingSamples, tapping_left.json.TappingSamples" \
	-p "syn26250286" \
	-v 1 \
	-q "LIMIT 50";
tapping_features_v2:
	Rscript feature_extraction/tapping/extract_mhealthtools_tapping_features.R \
	-g "~/git_token.txt" \
	-i "syn15673381" \
	-o "mhealthtools_20secs_clean_button_none_tapping_v2_features.tsv" \
	-n "run tapping feature extraction" \
	-q "LIMIT 50";
	