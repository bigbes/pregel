# TNT_PATH=/Users/blikh/src/work/tarantool/src/tarantool
TNT_PATH=tarantool
TNT_CTL=`which tarantoolctl`

$TNT_PATH $TNT_CTL stop worker-1
$TNT_PATH $TNT_CTL stop worker-2
$TNT_PATH $TNT_CTL stop worker-3
$TNT_PATH $TNT_CTL stop worker-4
$TNT_PATH $TNT_CTL stop worker-5
$TNT_PATH $TNT_CTL stop worker-6
$TNT_PATH $TNT_CTL stop worker-7
$TNT_PATH $TNT_CTL stop worker-8
