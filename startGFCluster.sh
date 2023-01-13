cdir=`pwd`
cd cluster
gfsh -e "start locator --name locatorGFRedis --dir locatorGFRedis"
gfsh -e "connect" -e "start server --name=serverGFRedis1 --dir=serverGFRedis1 --locators=localhost[10334] --server-port=40404 --classpath=$cdir/redisLib/* --J=-Dgemfire-for-redis-port=6378 --J=-Dgemfire-for-redis-enabled=true"
gfsh -e "connect" -e "start server --name=serverGFRedis2 --dir=serverGFRedis2 --locators=localhost[10334] --server-port=40405 --classpath=$cdir/redisLib/* --J=-Dgemfire-for-redis-port=6379 --J=-Dgemfire-for-redis-enabled=true"
cd ../

java -cp target/gemfire-redis-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.vmware.gemfire.redis.RedisClient