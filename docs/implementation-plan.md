# 文档监听插件实现计划

**目标：** 在当前独立目录实现 ES 8.16.1 可安装插件和 Kafka starter，附中文注释及验证说明。

**架构：** core 定义事件及异步派发；autoconfigure 管理配置与 Sink Bean；Kafka starter 提供 KafkaProducer；ES plugin 负责监听与插件生命周期。采用 Java 17 和 Maven 多模块构建。

- [x] 创建父 POM、Maven Wrapper 和模块构建，ES 依赖设为 provided，Spring 禁止打包日志实现。
- [x] 先写核心事件、主副本过滤、异步队列、关闭与异常测试，观察失败后实现。
- [x] 先写 Kafka MockProducer 和自动配置上下文测试，再实现 starter；每条事件等待 Kafka 确认以限制在途数量并维持出队顺序。
- [x] 核查 ES 8.16.1 源码后实现 Plugin、节点生命周期及分片回调，配置文件只从 ES config 目录读取。
- [x] 打包平铺插件 ZIP，包含描述文件和最小所需安全策略，不包含 ES 自身的运行库。
- [x] 编写中文 README、配置样例、扩展 starter 示例、Kafka 本地启动及多节点人工验证步骤。
- [x] 执行 clean verify，检查 ZIP 文件和依赖重复类；能够运行时增加真实 ES 插件加载及 Kafka 烟雾验证，明确未执行项目。

测试命令：`./mvnw clean verify`。预期全部测试通过并生成 `doc-listener-es-plugin/target/doc-listener-1.0.0.zip`。

实际完成：真实 ES 8.16.1 安装、启动及 broker 不可达时写入验证通过；真实 Kafka 收发和多节点验证未执行，见 verification.md。
