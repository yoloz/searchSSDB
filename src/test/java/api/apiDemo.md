
## 创建启动停止删除
```
curl localhost:12580/create -X POST -d "CREATE TABLE test(index int,city string,company text,english text,time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS'),timestamp long) name=listTest addr='127.0.0.1:8888' type=list"

curl localhost:12580/create -X POST -d "CREATE TABLE test(index int,city string,company text,english text,time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS'),timestamp long) name=listTest addr='127.0.0.1:8888' type=list analyser=StandardAnalyserIgnoreCase"

curl localhost:12580/stop -X POST -d "test"

curl localhost:12580/start -X POST -d "test"

curl localhost:12580/delAll -X POST -d "test"
```

##查询
```
curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where city='hangzhou' limit 10\"}"

curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where city='hangzhou' limit 10\",\"key\":2}"

curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where company like '北' limit 1\"}"

curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where index=1 limit 1\"}"
```

##过期查询
```
curl localhost:12580/query -X POST -d "{\"key\":\"select * from test where city='hangzhou' limit 10\"}"

curl localhost:12580/query -X POST -d "{\"key\":\"select * from test where city='hangzhou' order by index limit 10\"}"

curl localhost:12580/query -X POST -d "{\"key\":\"gMSWyOmTgS2CjW0lM8Xz9A\u003d\u003d\",\"offset\":2,\"limit\":10}"

curl localhost:12580/query -X POST -d "{\"key\":\"select * from test where company like '北' limit 1\"}"

curl localhost:12580/query -X POST -d "{\"key\":\"select * from test where index=1 limit 1\"}"
```