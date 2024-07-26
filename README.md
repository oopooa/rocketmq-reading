## Apache RocketMQ

[![Build Status][maven-build-image]][maven-build-url]
[![CodeCov][codecov-image]][codecov-url]
[![Maven Central][maven-central-image]][maven-central-url]
[![Release][release-image]][release-url]
[![License][license-image]][license-url]
[![Average Time to Resolve An Issue][percentage-of-issues-still-open-image]][pencentage-of-issues-still-open-url]
[![Percentage of Issues Still Open][average-time-to-resolve-an-issue-image]][average-time-to-resolve-an-issue-url]
[![Twitter Follow][twitter-follow-image]][twitter-follow-url]

**基于 RocketMQ 5.2.0-release 版本的源码解析**

### 目录结构

> ⭐ 表示重点内容

```text
rocketmq
 ├── acl (用户权限、安全、验证模块)
 ├── broker ⭐(RocketMQ 的 Broker, 实现消息存储、投递、查询等功能)
 ├── client ⭐(RocketMQ 的 Producer、Consumer、管理后台代码, 实现生产消息、消费消息、后台监控等)
 ├── common (公共的类和方法)
 ├── distribution (部署的一些脚本和配置文件)
 ├── example (简单使用的案例)
 ├── filter (消息过滤器)
 ├── namesrv ⭐(RocketMQ 的 NameServer, 实现注册中心和命名服务)
 ├── openmessaging (开发消息标准模块)
 ├── proxy ⭐(访问 Broker 的代理服务, 基于gRPC协议)
 ├── remoting ⭐(RocketMQ 的远程网络通信模块, 基于 Netty 实现)
 ├── srvutil (服务相关工具类)
 ├── store ⭐(Broker 的消息存储模块)
 ├── style (代码规范检查)
 ├── test (测试模块)
 ├── tieredstore (分层存储模块)
 └── tools (命令行监控工具)
```

### 学习计划

- 已完成 - ✅
- 进行中 - 🚧
- 未开始 - ❌

---

#### NameServer
1. 服务启动 ✅
2. 心跳监测 ❌

#### Broker
1. 服务启动 ✅
2. 加载文件消息及数据恢复 🚧
3. 消息接收 🚧

#### Producer
1. 服务启动 ❌
2. 消息发送 ❌

#### Consumer


---
🔧 Go to Issue
- https://github.com/apache/rocketmq/issues/{issue}
---
> 原文档 👇👇👇  
[Apache RocketMQ](https://github.com/apache/rocketmq)

[maven-build-image]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml/badge.svg
[maven-build-url]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml
[codecov-image]: https://codecov.io/gh/apache/rocketmq/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/apache/rocketmq
[maven-central-image]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg
[maven-central-url]: http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq
[release-image]: https://img.shields.io/badge/release-download-orange.svg
[release-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[average-time-to-resolve-an-issue-image]: http://isitmaintained.com/badge/resolution/apache/rocketmq.svg
[average-time-to-resolve-an-issue-url]: http://isitmaintained.com/project/apache/rocketmq
[percentage-of-issues-still-open-image]: http://isitmaintained.com/badge/open/apache/rocketmq.svg
[pencentage-of-issues-still-open-url]: http://isitmaintained.com/project/apache/rocketmq
[twitter-follow-image]: https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social
[twitter-follow-url]: https://twitter.com/intent/follow?screen_name=ApacheRocketMQ