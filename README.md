# 项目介绍
meta-feature-store    
该项目旨在打造一个开源的特征服务平台，基于很多公司都有自己的推荐业务场景，一个通用的特征服务平台可以帮助团队快速进行特征工程构建，并且可以借助特征服务平台进行特征共享，
告别烟囱式的开发模式，制定出一套标准的特征存储格式及搭建一套完整的特征监控体系，帮助业务快速实现业务目标，提高开发效率及数据质量。这个项目是算法MLops中的其中数据
流处理的核心模块，主要负责特征的自动化生产（实时、离线）、注册、管理、样本的构建、在线特征获取、特征管理平台前后端部分、在线学习等各个部分。以jar包的形式对外提供，
使用者只需要引用相关依赖即可使用
#整体架构
##项目概览
### feature-common
```
├── README.md
├── feature-common
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           │   └── com
│           │       └── meta
│           │           ├── entity  # 特征存储PB类
│           │           │   └── FeatureDTO.java
│           │           └── utils   # java工具包类  
│           │               └── ProtoStuffUtils.java
│           ├── resources
│           │   ├── feild_value.proto   # 特征存储pb文件定义，使用pb3协议
│           │   └── winutils.exe    # windows hadoop模拟环境工具，可以用来本地调试spark使用方法看./doc/guide.md
│           └── scala
│               └── com
│                   └── meta
│                       ├── conn    # 各类连接器
│                       │   ├── hbase   # hbase
│                       │   │   ├── HbaseConnectInfo.scala
│                       │   │   └── HbaseConnector.scala
│                       │   ├── mysql   # mysql
│                       │   ├── redis   # redis
│                       │   │   ├── JedisClusterName.scala
│                       │   │   └── JedisConnector.scala
│                       │   └── tdbank  #tdbank
│                       │       ├── TDBanReceiverConfig.scala
│                       │       └── TDbankStreamingContext.scala
│                       ├── entity  
│                       │   ├── FeatureTypeEnum.scala
│                       │   ├── RedisEnum.scala
│                       │   ├── SerializeTypeEnum.scala
│                       │   └── SerilizeUtils.scala
│                       └── featuremeta    # 特征元数据类构造
│                           ├── RedisFeatureInfo.scala
│                           ├── RedisFeatureMeta.scala
│                           ├── RedisFloatListMeta.scala
│                           ├── RedisFloatMeta.scala
│                           ├── RedisIntListMeta.scala
│                           ├── RedisIntMeta.scala
│                           ├── RedisMapFloatMeta.scala
│                           ├── RedisMapStringMeta.scala
│                           ├── RedisSeqListMeta.scala
│                           └── RedisStringMeta.scala
```
主要负责特征抽象相关SDK，包括特征构建、注册、存储、获取等核心API，规范特征存储格式及统一优化特征存储、入库、获取等；另外包括底层数据库访问、各类工具包
其中vector 相关操作
1.BLAS 实现向量BLAS操作，包括向量点乘
2.MLUtils 机器学习数值计算操作，包括向量链接，向量归一化、向量模长计算等操作

### feature-offline
```
├── feature-offline
│   ├── mlstudio-webserver.log
│   ├── pom.xml
│   ├── spark-warehouse
│   └── src
│       └── main
│           ├── java
│           ├── resources
│           └── scala
│               └── com
│                   └── meta
│                       └── sys    # 离线特征入库SDK,将sql转换成特征注册管理入库
│                           ├── Hive2RedisUtils.scala
│                           └── SparkMetaUtils.scala
```
 主要负责离线特征、样本生产的代码生产SDK,使用spark作为离线处理引擎，提供sql化的离线特征生产能力，是MLOps平台中离线特征生产平台的核心代码
### feature-online
```
├── feature-online
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           ├── resources
│           └── scala
│               ├── com
│               │   └── meta
│               │       ├── feature    # sparkStreaming tdbank 消费类
│               │       │   ├── SparkStreamingTDbank.scala
│               │       │   └── SparkStreamingTDbankConsumer.scala
│               │       ├── flink   # flink实时特征处理
│               │       ├── spark   # spark实时特征处理
│               │       │   ├── hbase   # hbase工具，rowkey设计、hbase2hdfs工具
│               │       │   │   ├── HbaseHistory2Hdfs.scala
│               │       │   │   ├── HbaseRowKeyUtils.scala
│               │       │   │   └── HbaseUtil.scala
│               │       │   ├── kafka   # 实时特征处理模板类，提供基础实时特征处理、序列特征、ctr统计特征,并且可以多流合并处理
│               │       │   │   ├── CtrStatStreaming.scala
│               │       │   │   ├── Kafka2HabseStreaming.scala
│               │       │   │   ├── Kafka2KafkaStreaming.scala
│               │       │   │   ├── KafkaAsynProcessingStreaming.scala
│               │       │   │   ├── KafkaProcessingStreaming.scala
│               │       │   │   ├── KafkaSourceStreaming.scala
│               │       │   │   ├── MultiSequenceEventStreaming.scala
│               │       │   │   ├── SequenceEventStreaming.scala
│               │       │   │   ├── ShowClickAggregator.scala
│               │       │   │   └── conn    # kafka连接器
│               │       │   │       ├── KafkaConnector.scala
│               │       │   │       └── KafkaSource.scala
│               │       │   └── monitor    # 监控模块，分别监控实时程序和离线程序
│               │       │       ├── SparkApplistener.scala
│               │       │       └── SparkMonitor.scala
│               │       ├── tfrecord
│               │       └── utils   # 工具包
│               │           ├── LocalSocketStreaming.scala
│               │           ├── SocketThread.scala
│               │           └── StreamingUtils.scala
│               └── org
│                   └── apache     # apache 相关类重写
│                       └── spark
│                           └── streaming
│                               └── dstream
│                                   └── DelayDStream.scala
```
主要负责实时特征处理模块，在线学习、实时特征生产SDK模块；特征类型包括实时序列特征、ctr统计特征、普通实时特征生产模板
### feature-flow
```
├── feature-flow
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           ├── resources
│           └── scala
│               └── com
│                   └── meta
│                       └── data
│                           ├── conf    # 流水线配置化类，包括hbase、特征处理、tfrecord处理等配置
│                           │   ├── HbaseInfoConfig.scala
│                           │   ├── MethodWrapper.scala
│                           │   ├── RedisFeatureMetaWrapper.scala
│                           │   ├── TFFeatureConfig.scala
│                           │   └── TransformerConf.scala
│                           ├── pipeline    # 特征xml配置文件解析流程
│                           │   ├── DataFlowConfigReader.scala
│                           │   ├── DataFlowDriver.scala
│                           │   └── Hbase2TFRecord.scala
│                           └── utils   # 工具包
│                               └── MLUtils.scala
```
主要负责特征平台数据流组件的常用操作，配置化特征获取，采用xml配置格式进行配置化特征获取以及线上线下一致性样本生产落库
### feature-web
```
├── feature-web
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           └── resources
├── guide.md
├── meta-feature-store.iml
├── pom.xml
├── scalastyle-config.xml
└── src
    └── main
        ├── java
        ├── resources
        └── scala
```
meta特征平台前后台部分，负责特征的展示、管理、特征集配置、模型特征获取配置等平台操作

# 快速上手
[开发指南](doc/guide.md)

# 行为准则
### 注意事项
1.redis写入限速规定：内存redis限速在最高10w qps, ssd限速在1w qps，redis稳定性至关重要，凡是涉及到redis操作都必须遵守，具体限速实现采用 guava RateLimiter 来实现

2.特征入库前进行 特征全量分布校验、默认值确认（0值最为常见，这里要求如果特征不存在直接null,不要用0值填充，方便入库默认值过滤），特征生产逻辑校验、特征表模式确认（增量表、全量表，建议全量表）

3.特征入库分区依赖逻辑：入库前必须确认：当天分区是否有数据，如果没有发送告警信息并暂停入库

4.特征入库前必须进行资源占用预估，防止redis内存不足导致线上事故

5.特征生产任务可能由多方制作，最后入库时统一交接到特征工程组维护，防止数据丢失

6.特征刷入之前将前四步信息写在对应tapd单最终来审核

7.特征刷入之后，需经过测试进行特征抽样、数据量测试

# 注意事项
# 常见问题 FAQ
很多时候，会因为一时疏忽导致某个地方设置出现错误，而导致运行报错。这类错误通常十分隐秘，不易排查，甚至会浪费大量时间。这里给出一份检查手册，发现报错后建议首先花几分钟检查手册上的内容是否设置正确，以免浪费更多时间。
1. service-api(lib目录)安装到本地mvn库：mvn install:install-file -Dfile=lib/service-api-3.40.0.jar -DgroupId=service-api -DartifactId=service-api -Dversion=3.40.0 -Dpackaging=jar
2. settings.xml可以替换本地maven的settings文件；
3. 本地运行报错，检查是否勾选了“local”这个profile
4. 线上运行报错，检查是否取消勾选了“local”这个profile
5. 线上运行报错，检查在Tesla平台上是否正确选择tdw版本 —— tdw3.12 - 2.x
6. 如果向表中插入数据，检查是否保证数据与表的字段顺序对应一致
# 如何加入
# 备注




 