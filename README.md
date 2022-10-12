# 项目介绍
meta-feature-store    
该项目旨在打造一个开源的特征服务平台，基于很多公司都有自己的推荐业务场景，一个通用的特征服务平台可以帮助团队快速进行特征工程构建，并且可以借助特征服务平台进行特征共享，
告别烟囱式的开发模式，制定出一套标准的特征存储格式及搭建一套完整的特征监控体系，帮助业务快速实现业务目标，提高开发效率及数据质量。这个项目是算法MLops中的其中数据
流处理的核心模块，主要负责特征的自动化生产（实时、离线）、注册、管理、样本的构建、在线特征获取、特征管理平台前后端部分、在线学习等各个部分。以jar包的形式对外提供，
使用者只需要引用相关依赖即可使用
###整体架构
###项目概览
```
├── README.md
├── feature-common
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           │   └── com
│           │       └── meta
│           │           ├── entity
│           │           │   └── FeatureDTO.java
│           │           └── utils
│           │               └── ProtoStuffUtils.java
│           ├── resources
│           │   ├── feild_value.proto
│           │   └── winutils.exe
│           └── scala
│               └── com
│                   └── meta
│                       ├── conn
│                       │   ├── hbase
│                       │   │   ├── HbaseConnectInfo.scala
│                       │   │   └── HbaseConnector.scala
│                       │   ├── mysql
│                       │   ├── redis
│                       │   │   ├── JedisClusterName.scala
│                       │   │   └── JedisConnector.scala
│                       │   └── tdbank
│                       │       ├── TDBanReceiverConfig.scala
│                       │       └── TDbankStreamingContext.scala
│                       ├── entity
│                       │   ├── FeatureTypeEnum.scala
│                       │   ├── RedisEnum.scala
│                       │   ├── SerializeTypeEnum.scala
│                       │   └── SerilizeUtils.scala
│                       └── featuremeta
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
│                           ├── conf
│                           │   ├── HbaseInfoConfig.scala
│                           │   ├── MethodWrapper.scala
│                           │   ├── RedisFeatureMetaWrapper.scala
│                           │   ├── TFFeatureConfig.scala
│                           │   └── TransformerConf.scala
│                           ├── pipeline
│                           │   ├── DataFlowConfigReader.scala
│                           │   ├── DataFlowDriver.scala
│                           │   └── Hbase2TFRecord.scala
│                           └── utils
│                               └── MLUtils.scala
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
│                       └── sys
│                           ├── Hive2RedisUtils.scala
│                           └── SparkMetaUtils.scala
├── feature-online
│   ├── pom.xml
│   └── src
│       └── main
│           ├── java
│           ├── resources
│           └── scala
│               ├── com
│               │   └── meta
│               │       ├── feature
│               │       │   ├── SparkStreamingTDbank.scala
│               │       │   └── SparkStreamingTDbankConsumer.scala
│               │       ├── flink
│               │       ├── spark
│               │       │   ├── hbase
│               │       │   │   ├── HbaseHistory2Hdfs.scala
│               │       │   │   ├── HbaseRowKeyUtils.scala
│               │       │   │   └── HbaseUtil.scala
│               │       │   ├── kafka
│               │       │   │   ├── CtrStatStreaming.scala
│               │       │   │   ├── Kafka2HabseStreaming.scala
│               │       │   │   ├── Kafka2KafkaStreaming.scala
│               │       │   │   ├── KafkaAsynProcessingStreaming.scala
│               │       │   │   ├── KafkaProcessingStreaming.scala
│               │       │   │   ├── KafkaSourceStreaming.scala
│               │       │   │   ├── MultiSequenceEventStreaming.scala
│               │       │   │   ├── SequenceEventStreaming.scala
│               │       │   │   ├── ShowClickAggregator.scala
│               │       │   │   └── conn
│               │       │   │       ├── KafkaConnector.scala
│               │       │   │       └── KafkaSource.scala
│               │       │   └── monitor
│               │       │       ├── SparkApplistener.scala
│               │       │       └── SparkMonitor.scala
│               │       ├── tfrecord
│               │       └── utils
│               │           ├── LocalSocketStreaming.scala
│               │           ├── SocketThread.scala
│               │           └── StreamingUtils.scala
│               └── org
│                   └── apache
│                       └── spark
│                           └── streaming
│                               └── dstream
│                                   └── DelayDStream.scala
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
### feature-common
主要负责特征抽象相关SDK，包括特征构建、注册、存储、获取等核心API，规范特征存储格式及统一优化特征存储、入库、获取等；另外包括底层数据库访问、各类工具包
其中vector 相关操作
1.BLAS 实现向量BLAS操作，包括向量点乘
2.MLUtils 机器学习数值计算操作，包括向量链接，向量归一化、向量模长计算等操作
### feature-offline
 主要负责离线特征、样本生产的代码生产SDK,使用spark作为离线处理引擎，提供sql化的离线特征生产能力，是MLOps平台中离线特征生产平台的核心代码
### feature-online
主要负责实时特征处理模块，在线学习、实时特征生产SDK模块；特征类型包括实时序列特征、ctr统计特征、普通实时特征生产模板
### feature-flow
主要负责特征平台数据流组件的常用操作，配置化特征获取，采用xml配置格式进行配置化特征获取以及线上线下一致性样本生产落库

### feature-web
meta特征平台前后台部分，负责特征的展示、管理、特征集配置、模型特征获取配置等平台操作


# 快速上手
[开发指南](doc/guide.md)

# 行为准则
# 注意事项
# 常见问题 FAQ
# 如何加入
# 备注




 