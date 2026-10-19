# ES 文档变化监听插件设计

## 目标

在当前目录创建独立工程，提供可安装到 Elasticsearch 各数据节点的插件 ZIP。通过 Spring Boot 自动配置和 starter 依赖扩展外部接收系统，首个接收系统为 Kafka。代码注释、配置说明和验证指南使用中文。

## 版本基线

已检查本机 ES 源码的 build-tools-internal/version.properties，版本为 8.1.0，但用户确认实际节点为 8.16.1，故本工程固定针对 ES 8.16.1；插件描述文件和编译依赖必须与节点版本匹配。当前 shell 默认 Java 为 26-ea，未找到 mvn 命令；实现阶段需要定位可用的构建工具与兼容 JDK。

## 推荐架构：节点插件内加载 starter

- doc-listener-core：文档事件模型、ChangeSink 扩展接口、有界异步队列、分发与关闭生命周期。
- doc-listener-spring-boot-autoconfigure：加载插件配置，自动发现 ChangeSink Bean，使用无 Web 的 Spring 上下文；不启动 SpringApplication 或接管 ES 日志。
- doc-listener-spring-boot-starter-kafka：引入自动配置和 Kafka 接收器，使用 KafkaProducer 发送 JSON 事件；新增其他系统时增加对应 starter。
- doc-listener-es-plugin：实现 Plugin 和 IndexingOperationListener，管理 Spring 上下文与分发器，打包插件描述文件、安全策略及运行时依赖。ES 与其已有依赖不重复打包，检查 jar hell。

starter 的引入发生在插件工程编译阶段；修改 starter 依赖后重新打包并部署所有相关节点，不支持运行中热插拔。

## 事件和监听范围

仅处理 PRIMARY 来源且引擎执行成功的文档写入，以及确实找到文档的成功删除。支持按索引匹配规则筛选。新增与覆盖使用 INDEX 事件和 created 标记，不把覆盖写入误称为用户调用了 Update API。

事件包含索引名与 UUID、分片 ID、文档 ID、routing（操作可用时）、seqNo、primaryTerm、事件类型、写入后的 source（写入事件）、时间。使用索引 UUID、分片 ID 和 primaryTerm、seqNo 组合形成事件标识，Kafka key 使用索引 UUID、分片 ID 和文档 ID，供消费端去重和分区。

不提供旧文档内容；noop 更新、删除索引、历史文档回填不在范围内。主分片本地成功不等于客户端请求最终成功，不等于已经 refresh。

## 发送与故障语义

监听回调只构造不可变事件并尝试入队，Kafka 发送由后台工作线程执行。队列有界；默认队列满时丢弃并限频告警，记录丢弃计数。Kafka 发送失败记录错误及计数；启用 producer 自身的重试与幂等发送。

该首版是尽力投递：节点崩溃、队列满或持续发送失败可能丢失事件，主分片重试等情况也可能产生重复。没有本地持久化 outbox，不宣称至少一次或恰好一次交付。关闭时在配置的期限内尽力排空并关闭 producer。

## 配置和部署

提供中文注释的配置样例，包含启用开关、索引过滤、队列容量、Kafka bootstrap.servers、topic、关闭等待时间及 Kafka 客户端参数。Kafka 认证配置与 TLS 参数通过受控客户端配置入口传递，不输出凭据。

提供构建命令、ZIP 安装命令、滚动重启步骤、Kafka 消费命令，以及新增、覆盖、update、noop、删除、bulk 和副本不重复发送的人工验证步骤。

## 验证

单元测试覆盖成功/失败/副本过滤、删除不存在文档、事件数据、队列满、发送异常和关闭行为；Kafka MockProducer 验证发送内容及异步失败；Spring 上下文测试验证 starter 自动装配和扩展接收器。构建生成 ZIP 后检查描述文件、依赖冲突及安全策略。实际多节点安装由用户执行，未执行的集群测试明确列出。

## 备选方案

1. 纯 Java SPI 插件：依赖和权限更简单，但不提供 Spring starter 的自动装配体验。
2. 插件加独立 Spring Boot 转发服务：外部系统扩展与 ES 隔离，但多一个部署组件，且仍需定义节点到转发服务的可靠传输。

## 已确认决策

用户已批准插件进程内加载 starter 的方案。使用 Java 17 编译、Spring Boot 2.7.18 / Spring Framework 5.3.31 自动配置、Kafka clients 3.8.1，提供 Maven Wrapper；不改动原 ES 源码仓库。配置置于 ES 配置目录下的 doc-listener.properties，避免将 Kafka 凭据写入 elasticsearch.yml。

## 实测兼容性调整

Spring Boot 3.3.5/Spring 6 的默认资源扫描和配置绑定触发 accessSystemModules，而该权限不在 ES 8.16.1 插件允许列表。最终使用 Spring Boot 2.7.18 / Spring Framework 5.3.31，并有受限权限子 JVM 回归测试。Kafka app-info MBean 需要 MBeanServerPermission、MBeanPermission 和 MBeanTrustPermission，已通过真实安装与启动验证。不申请 modifyThread/modifyThreadGroup，这些不是 ES 经典插件允许声明的权限。
