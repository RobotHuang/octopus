# octopus

## Benchmark
### environment
OS: Centos7  
cpu: Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz  
lib: redigo gorm go-ceph  
Ceph version: nautilus  
MySQL version: 8.0.23 MySQL Community Server  
Redis version: 6.2.1
### Result
#### Ceph Object  
|benchmark|times|average time|  
|--|--|--|
|BenchmarkRadosWriteObject100|1000000000|0.2808 ns/op|  
|BenchmarkRadosReadObject100|1000000000|0.01171 ns/op| 
 
#### Ceph Xattr  
|benchmark|times|average time|  
|--|--|--|
|BenchmarkRadosSetXattr100|1000000000|0.3441 ns/op| 
|BenchmarkRadosGetXattr100|1000000000|0.01197 ns/op| 

#### Ceph Omap
|benchmark|times|average time|  
|--|--|--|
|BenchmarkRadosSetOmap100|1000000000|0.2314 ns/op|  
|BenchmarkRadosGetOmap100|1000000000|0.06526 ns/op|

#### Redis String  
|benchmark|times|average time|  
|--|--|--|
|BenchmarkRedisPutMetadata100|1000000000|0.007100 ns/op|  
|BenchmarkRedisGetMetadata100|1000000000|0.006686 ns/op|  

#### MySQL
|benchmark|times|average time|  
|--|--|--|
|BenchmarkMySQLGetMetadata100|1000000000|0.05409 ns/op|  
|BenchmarkMySQLGetMetadata100|1000000000|0.03049 ns/op|  
(MySQL needed to be tested further)  

||benchmark|times|average time|
|--|--|--|--|
|1|BenchmarkGetObjectWithCache|1543|788967 ns/op|
|2|BenchmarkGetObjectWithCache|1672|734345 ns/op|
|3|BenchmarkGetObjectWithCache|1616|726735 ns/op|
|1|BenchmarkGetObject|902|1301964 ns/op|