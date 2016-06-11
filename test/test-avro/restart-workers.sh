# TNT_PATH=/Users/blikh/src/work/tarantool/src/tarantool
TNT_PATH=tarantool
TNT_CTL=`which tarantoolctl`

$TNT_PATH $TNT_CTL restart worker-1
$TNT_PATH $TNT_CTL restart worker-2
$TNT_PATH $TNT_CTL restart worker-3
$TNT_PATH $TNT_CTL restart worker-4
$TNT_PATH $TNT_CTL restart worker-5
$TNT_PATH $TNT_CTL restart worker-6
$TNT_PATH $TNT_CTL restart worker-7
$TNT_PATH $TNT_CTL restart worker-8
