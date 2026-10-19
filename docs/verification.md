# 验证记录

日期：2026-10-19（北京时间）。本工程为目标目录中的独立 Maven 工程，未修改原 ES 源码仓库。

## 构建与自动化测试

命令：`JAVA_HOME=<JDK17> ./mvnw -B -ntp clean verify`。

结果：BUILD SUCCESS，30 项测试，0 失败、0 错误。包括 JSON 大整数、带 routing 的文档身份、主副本/恢复过滤、失败写入、删除不存在文档、有界队列、限时关闭、接收器隔离、Kafka 异步确认与错误、starter 发现、自定义 Sink 扩展及受限权限子 JVM 回归。

构建输出见 `verification-evidence/verify.log`。

## ZIP 校验

使用 `scripts/check-plugin-zip.py` 检查平铺结构、描述文件 ES 8.16.1 / Java 17、插件内部重复类，以及与真实 ES 8.16.1 lib 目录重复类，均通过。不打包 Elasticsearch/Lucene/Log4j API/Core 自身依赖。

最终 ZIP SHA-256：`80eba8e164d71eeae84c4691534e857d814053a83649cd536f5556d476904fce`。

## 真实 ES 8.16.1 烟雾验证

从本机已有官方 8.16.1 发行目录克隆到 `/tmp/doc-listener-es-8.16.1-smoke`，使用独立数据目录、日志目录、集群名和 127.0.0.1:19216 端口启动，未修改已有节点。运行环境为 JDK 21、512 MiB 堆；仅此临时实例关闭 ES 安全认证。

- 官方 elasticsearch-plugin 安装器接受 ZIP 和最终安全策略。
- 启动时成功加载 doc-listener 插件、Spring 上下文、KafkaProducer；未出现 accessSystemModules 或 MBean 权限错误。
- `/_nodes/plugins` 返回 name=doc-listener、version=1.0.0；根 API 返回 ES 8.16.1。
- Kafka bootstrap 指向没有 broker 的 127.0.0.1:19092，用于验证外部不可用时的隔离。
- 执行 `scripts/smoke.sh`：新增、覆盖、update、noop、delete、删除不存在文档、bulk 操作均返回预期结果；bulk errors=false。
- 节点统计：`Stats[accepted=6, sent=0, failed=6, dropped=0, queued=0]`，正好对应六次实际文档变化，noop 和删除不存在文档没有发送。
- SIGINT 关闭临时 ES，插件排空队列、关闭 producer 和 Spring 上下文，输出最终 stats。临时节点已停止。

安装、请求结果与节点摘要见 `verification-evidence/`。真实节点验证后，审查将 Kafka key 从 `[indexUuid,id]` 加强为 `[indexUuid,shardId,id]`，该修改已通过最终构建与回归测试。

## 未执行项目

本机 Docker CLI 存在，但 Docker daemon 未运行，因此没有启动真实 Kafka broker，也没有执行真实成功消息消费。Kafka 成功与异步失败由 MockProducer 测试覆盖，不替代 broker 集成测试。

未执行多节点主副本分布、主分片迁移、故障切换及滚动升级验证；未验证 SASL/TLS、生产 Kafka 版本组合和吞吐压测。README 提供验证步骤，需在目标环境补充。首版仍是尽力投递，不提供持久化 outbox、断点续传或恰好一次交付。
