# searchSSDB

### 介绍
通过给ssdb的内容创建索引(lucene),进而使用sql语句(转换为lucene query)查询

### 目录结构
```
project
│   README.md
│   build.gradle
│   settings.gradle
└───bin
│   │   start.sh  #启动
│   │   stop.sh   #停止
│   │   checkEnv.sh
│
└───conf
|   │   analyser.properties
|   │   server.properties
|
└───src
    |
    └───main
    |      │   ...
    |
    └───test
           │   ...
   
```

### 配置文件说明
conf/server.properties
```
indexDir=.var    #索引创建目录,默认项目本地var
httpPort=12580   #工具对外api访问端口
totalIndex=6     #允许运行的索引实例数(LRU)。超过LRU原则关闭最旧的index实例，默认工具启动分配Xmx=1G
indexBuffer=128  #每个index实例的RAMBufferSize(MB)
perDayHour=2     #每天定时commit(0-23),避免长时间运行被断电等异常情况造成的损失
searchExpired=0  #近实时搜索的有效期(即搜索存活多久)默认为0(近实时搜索非分页模式)(单位:s)
```
  
> * totalIndex实例数<(工具Xmx/indexBuffer)

conf/analyser.properties
```
StandardAnalyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
StandardAnalyserIgnoreCase=index.analyser.StandardAnalyserIgnoreCase
```
> * key大小写不敏感;
> * 映射只对更新后的创建起作用,先前的已经创建的不影响；
> * 支持热更新,下文会有api；

### API说明
失败返回格式：{"success":false,"error":"errorMsg"}"

  url|说明
 :---:|:---:
 /create|创建
 /query|查询
 /start|启动
 /stop|停止
 /getAll|获取创建的索引
 /delAll|删除
 /updateAnalyser|更新分析器

* /create

POST indexName  
返回：{"success":true}

* /query

POST json {sql:""\[,key:\]}
1. 直接查询`{"sql":"select * from test where city='hangzhou'"}`,适用于离线和近实时搜索;  
> 添加 limit page,count,如：    
select * from test where city='hangzhou' limit 1,15(取第一页数据15条);      
select * from test where city='hangzhou' limit 1000(取前1000数据);  
order by,group by见下文API用例
2. 近实时搜索分页查询`{"sql":"select * from test where city='hangzhou' limit 3,15","key":2}`
> 使用key=2(近实时搜索直接查询返回值)的searcher继续取第三页15条数据,如此可以保持total及数据不变;    
离线(index实例未运行)分页直接搜索即可;

返回：
1. 索引未运行  
{"total":1000,"results":\[{}...\],"success":true}
2. 索引运行中  
{"total":1000,"results":\[{}...\],"key":2,"success":true}
3. 分组查询  
{"total":3,"results":\[{"groupName":"","count":200}...\],"success":true}  
{"total":3,"results":\[{"groupName":"","count":200}...\],"key":2,"success":true}  
> total为此次搜索匹配的总数,分组查询中为总组数;  
key为此次的searcher,分页展示时继续使用同一searcher,非分页模式返回-1,在索引运行时才有;    
分组查询中仅返回分组列及组内数据量;    

* /start

POST indexName  
返回：{"success":true}

* /stop

POST indexName  
返回：{"success":true}

* /getAll

GET  
返回：{"success":true,"indexes":\[{"indexName":"abc","ssdbAddr":"127.0.0.1:12","ssdbName":"abc"},...\],"running":\["b",...\]}
> indexName:索引名称  
ssdbAddr:ssdb地址  
ssdbName:索引对应的ssdb列表名称

* /delAll

POST indexName  
返回：{"success":true}

* /updateAnalyser

POST  
返回：{"success":true}

### API用例

* 创建:
```
curl localhost:12580/create -X POST -d "CREATE TABLE test(index int,city string,company text,time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS'),timestamp long) name=listTest addr='127.0.0.1:8888' type=list analyser=StandardAnalyserIgnoreCase"

curl localhost:12580/create -X POST -d "CREATE TABLE test(index int,city string,company text,time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS'),timestamp long) name='list*' addr='127.0.0.1:8888' type=list"
```
> analyser:分词实现,可以不配置,默认是StandardAnalyzer,只需要填写analyser.properties配的键值即可;  
name:数据源名称,暂时只支持ssdb,name支持前缀匹配;  
addr:地址和端口;  
type:list或hash;  
字段类型:int,long,date,string,text.string不分词,text会分词;

* 停止:

`curl localhost:12580/stop -X POST -d "test"`create操作是不停止的,这个请求可以停止索引并将数据写进磁盘;

* 启动:

`curl localhost:12580/start -X POST -d "test"`

* 删除:

`curl localhost:12580/delAll -X POST -d "test"`删除索引

* 搜索:
```
#索引运行中分页取数据:
curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where city='hangzhou' limit 1,10\"}"
#上述,取第一页10条数据,返回结果:{"total":1000,"results":[{}...],"key":12,"success":true},这时需要取第二页数据,如下：
curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where city='hangzhou' limit 2,10\",\"key\":12}"

curl localhost:12580/query -X POST -d "{\"sql\":\"select city from test where company like '北' order by index limit 1000\"}"

curl localhost:12580/query -X POST -d "{\"sql\":\"select * from test where company like '北' group by city order by city desc,index desc limit 1000\"}"
```
> 支持条件and,or,like,=,>,>=,<,<=,between,order by;  
条件>,>=,<,<=,between限定用于date,int,long;  
order by限定用于int,long,date,string;  
group by限定用于int,long,date,string且只支持一组;
>>>>>>> fea_audit
