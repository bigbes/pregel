TNT_PATH=/Users/blikh/src/work/tarantool/src/tarantool
TNT_CTL=`which tarantoolctl`

$TNT_PATH $TNT_CTL restart worker-1
$TNT_PATH $TNT_CTL restart worker-2
$TNT_PATH $TNT_CTL restart worker-3
$TNT_PATH $TNT_CTL restart worker-4
