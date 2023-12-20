# open-mq
[![Gitter](https://badges.gitter.im/brokercap-Bifrost/Bifrost.svg)](https://gitter.im/brokercap-Bifrost/Bifrost?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/brokercap/Bifrost.svg?branch=v1.7.x)](https://travis-ci.org/brokercap/Bifrost)
[![License](https://img.shields.io/github/license/jc3wish/Bifrost.svg)](https://opensource.org/licenses/apache2.0)

## 一款简单、使用便捷的消息队列，模仿RocketMQ，支持以下特性：

| 模块名              | 说明                       |
| ------------------- | -------------------------- |
| broker      | 消息存储、消息转发       |
| store      | 消息持久化        |
| client    | producer、consume都属于client         |
| controller    | 负责Broker Master的选举和通知Broker|
| namesrv      |元数据存储，原zk角色         |
| remoting      | 远程通信模块       |
| proxy   | Grpc proxy      |
| common | 通用能力、工具|
| demo |一些demo示例|

##快速开始
###源码启动
step 1、 启动 namesrv  
step 2、 启动 controller
