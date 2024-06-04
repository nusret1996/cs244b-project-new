LAUNCH_TIME=$(date -v+2S -u +%H:%M:%S)
echo "LAUNCH_TIME $LAUNCH_TIME"
osascript -e 'tell app "Terminal" to do script "cd /Users/sherylhsu/school/cs244b/cs244b-project/build_dir; ./main '"${LAUNCH_TIME}"' 200 ../peerconfig 0"'
osascript -e 'tell app "Terminal" to do script "cd /Users/sherylhsu/school/cs244b/cs244b-project/build_dir; ./main '"${LAUNCH_TIME}"' 200 ../peerconfig 1"'

osascript -e 'tell app "Terminal" to do script "cd /Users/sherylhsu/school/cs244b/cs244b-project/build_dir; ./main '"${LAUNCH_TIME}"' 200 ../peerconfig 2"'

 ./main '"$(date -v+2S -u +%H:%M:%S)"' 200 ../peerconfig 0