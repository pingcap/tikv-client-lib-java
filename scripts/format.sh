#!/bin/sh
if hash google-java-format 2>/dev/null; then 
	files_need_format=()
	files=$(find ../src -name "*.java")
	for file in "${files[@]}";do
		echo "formatting..."
		#google-java-format -r ("${file}")
		google-java-format -r ${file}
	done
else
	echo "google-java-format is not installed, please install it first"
	echo "If you are using macos, type 'brew install google-java-format'"
	echo "Otherwise, go to google-java-format github page:
	https://github.com/google/google-java-format.git
   	and download it."
fi
