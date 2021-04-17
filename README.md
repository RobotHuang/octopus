# octopus

## Benchmark
### environment
cpu: Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz  
redigo gorm go-ceph  
### Result
* Ceph Object  
BenchmarkRadosWriteObject100    1000000000               0.2808 ns/op
BenchmarkRadosReadObject100     1000000000               0.01171 ns/op  
* Ceph Xattr  
BenchmarkRadosSetXattr100       1000000000               0.3441 ns/op
BenchmarkRadosGetXattr100       1000000000               0.01197 ns/op
* Ceph Omap
BenchmarkRadosSetOmap100        1000000000               0.2314 ns/op  
BenchmarkRadosGetOmap100        1000000000               0.06526 ns/op  
* Redis String  
BenchmarkRedisPutMetadata100    1000000000               0.007100 ns/op  
BenchmarkRedisGetMetadata100    1000000000               0.006686 ns/op  
* MySQL
BenchmarkMySQLGetMetadata100    1000000000               0.05409 ns/op  
BenchmarkMySQLGetMetadata100    1000000000               0.03049 ns/op  
(MySQL needed to be tested further)  