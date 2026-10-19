# ES 文档变化监听插件

针对 **Elasticsearch 8.16.1** 的经典 Java 插件，使用 `IndexingOperationListener` 监听成功的主分片文档操作，通过 Spring Boot starter 扩展外部接收系统，当前实现 Kafka。代码、配置和验证步骤均包含中文说明。

## 工程结构

| 模块 | 职责 |
| --- | --- |
| `doc-listener-core` | 不可变事件、`ChangeSink` 接口、有界后台队列、投递统计 |
| `doc-listener-spring-boot-autoconfigure` | 配置绑定、starter 自动发现、派发器与资源关闭 |
| `doc-listener-spring-boot-starter-kafka` | Kafka JSON 消息发送与异步确认 |
| `doc-listener-es-plugin` | ES 8.16.1 生命周期、分片监听、ZIP 打包 |

Spring Boot 2.7.18 / Spring Framework 5.3.31 仅用于自动配置；不启动 Web 服务、不运行 `SpringApplication`、不接管 ES 日志。插件中的日志写入 ES Log4j2。starter 在**编译时**引入，添加或移除后需重新打包并在节点重装，不能运行中热插拔。

## 版本兼容说明

ES 8.16.1 的经典插件权限白名单禁止 `accessSystemModules`。实测 Spring 6 默认资源扫描器在初始化时调用 `ModuleFinder.ofSystem()`，因此本工程选择 Spring Boot 2.7.18 / Spring 5.3.31，而不是 Spring Boot 3。不要只改版本号升级到 Spring 6，需要重新验证资源扫描与配置绑定。该 Spring 版本已较旧，生产使用应纳入依赖安全评估和维护计划；若需要使用更新的 Spring 版本，建议将转发服务移到 ES 进程外。

当前验证使用 JDK 17 编译与 JDK 21 启动 ES。插件申请的 MBean 权限用于 Kafka 自身的 app-info 和指标注册，不会启动 Spring JMX 服务器。

## 构建

使用 JDK 17（推荐）或 JDK 21；输出 Java 17 字节码。Maven Wrapper 首次运行需要网络下载 Maven 和依赖。不要用 JDK 26 运行 ES 8.16.1。

```bash
cd /Users/levi/develop/project/my/es/plugin/docListener
# macOS 示例，其他系统设置为实际 JDK 路径
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
./mvnw clean verify
python3 scripts/check-plugin-zip.py
```

产物：`doc-listener-es-plugin/target/doc-listener-1.0.0.zip`。

ES 依赖使用 provided，不随插件重复打包。ZIP 内平铺依赖 JAR、描述文件和安全策略，不生成 Spring Boot fat JAR。可额外运行 `python3 scripts/check-plugin-zip.py <zip> <ES_HOME>/lib` 与真实节点依赖对比。

## 每个节点的安装与配置

1. 将 ZIP 复制到目标节点。所有可能承载目标索引分片的节点都需要安装；为了部署一致，可在集群所有节点安装。非数据节点没有分片回调，不会代替数据节点发送。
2. 按集群运维流程逐个停止节点，在各节点执行：

   ```bash
   "$ES_HOME/bin/elasticsearch-plugin" install file:///绝对路径/doc-listener-1.0.0.zip
   ```

   安装器会展示安全权限，确认后安装。更新已安装插件时需停节点、先 `remove doc-listener` 再安装新 ZIP；保留配置文件备份。
3. 将 `config/doc-listener.properties` 复制到该节点实际配置目录（`$ES_PATH_CONF`，未设置时为 `$ES_HOME/config`）。示例默认启用，必须修改 Kafka 地址及索引规则。配置文件缺失时插件禁用。文件使用 UTF-8，凭据文件设置为仅 ES 用户可读。
4. 启动节点并观察日志：`doc-listener 已启动`。所有节点确认版本与配置一致后进行写入验证。配置不热更新，修改需重启。
5. 使用 `GET /_nodes/plugins` 或每个节点的 `elasticsearch-plugin list` 确认安装。仅安装在协调节点或主节点不能覆盖全部文档变化。

插件网络策略当前允许外连任意 broker；上线前可在 `doc-listener-es-plugin/src/main/plugin-metadata/plugin-security.policy` 收紧目标地址，注意 Kafka 会连接 metadata 返回的每个 broker，而不只是 bootstrap 地址。TLS 信任库放在 ES config 目录，其他路径需要对应文件权限。SASL/TLS 尚未实测，SASL 登录通常还需要在策略中显式增加对应 AuthPermission，不能直接视为已支持验证的部署配置。自定义 starter 若需要额外权限，也需调整策略并重新打包；未授予 AllPermission。

## 本地 Kafka 和写入验证

本地需 Docker。以下 Kafka 只开放本机 9092，不应直接用于生产。远程 ES 节点使用它时，应将 advertised listeners 改成所有 ES 节点都能访问的地址。

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic es-document-changes --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic es-document-changes \
  --from-beginning --property print.key=true
```

另开终端，在安装插件的 ES 集群执行：

```bash
# 实际安全集群请设置 HTTPS 地址、CA 信任及认证；不要在 shell 历史里明文保存密码。
export ES_URL=http://localhost:9200
# export ES_USER=elastic
# read -r -s ES_PASSWORD; export ES_PASSWORD
./scripts/smoke.sh
```

使用全新测试索引和全新消费位点时，预期 **6 条事件**：新增、覆盖、update、delete 各一条，bulk 两条。noop 更新、删除不存在文档以及副本应用不产生事件。测试脚本会创建专用 `doc-listener-test` 索引，若已存在则停止，防止混入旧状态。脚本不自动删除索引。

多节点验证时确认测试索引主副本分布在不同节点：`GET /_cat/shards/doc-listener-test?v`。节点滚动重启、分片迁移后继续写入，检查新主分片上的插件仍发送。仅过滤副本不代表整个系统恰好一次；消费端仍需去重。

## 事件格式

```json
{
  "schemaVersion": 1,
  "eventId": "索引UUID:分片ID:primaryTerm:seqNo",
  "index": "doc-listener-test",
  "indexUuid": "索引UUID",
  "shardId": 0,
  "id": "1",
  "routing": null,
  "seqNo": 12,
  "primaryTerm": 1,
  "type": "INDEX",
  "created": false,
  "timestamp": 1730000000000,
  "source": {"name": "中文覆盖", "count": 2}
}
```

`type` 为 `INDEX` 或 `DELETE`；`created` 表示本次引擎写入是否创建文档，不表示用户一定调用了哪个 REST API。`source` 是本次写入快照，删除事件为 null；删除回调也拿不到 routing。`timestamp` 是节点回调的毫秒时间，不用于全局排序。`seqNo` 和 `primaryTerm` 取引擎执行结果，不能用写入前尚未分配的序号。

Kafka key 为 `[indexUuid,shardId,id]` 的 JSON 数组。同 ID 可因不同 routing 存在于不同分片，消费端必须将分片纳入文档身份；删除事件无法提供 routing，但保留 shardId。同一文档通常进入同一分区，但增减 Kafka topic 分区后分区映射可能改变。多个节点回调、并发写入及分片切换不提供跨节点的到达顺序保证。消费端按业务需求使用 seqNo/primaryTerm 处理重复及过期操作；主任期纳入 eventId，避免故障切换时同序号不同操作碰撞。

## 投递语义和运行边界

- 仅成功的 PRIMARY 操作，副本、恢复、失败操作被过滤。覆盖写入和实际产生变化的 update 都进入 INDEX；脚本 update 删除进入 DELETE。
- noop 更新、整索引删除、ILM 删除索引、快照恢复和历史回填不逐文档通知。监听器只覆盖实际经过该引擎回调的操作，不替代审计或完整 CDC。
- 本地主分片引擎成功不等于客户端最终成功，也不等于完成 refresh。外部事件可能先于客户端响应或搜索可见性。
- **尽力投递**：内存队列不持久化，宕机、超大事件、队列满、发送失败、关闭超时都可能丢失。没有应用层无限重试或断点续传，不保证至少一次或恰好一次。
- Kafka 开启 producer 幂等、`acks=all`，每个 Sink 的发送等待确认；这些不能消除 ES 重试、进程重启及故障切换导致的业务重复。失败计数表示失败/结果不确定，超时后消息仍可能实际到达。
- 单后台线程有意限制复杂度与在途请求，多 Sink 依次投递，某个 Sink 慢会拖慢其他 Sink，ES 写入线程只承担筛选、source 快照和尝试入队开销。压测后按需调整队列和超时。
- 每分钟输出 stats；关闭时输出最终 stats。`accepted`、`dropped` 按事件计数；`sent`、`failed` 按 Sink 尝试计数；快照构造异常单独限频计数告警。队列错误日志不打印 source 或异常详情，但 Kafka 自身也可能记录客户端配置，请保护节点日志。
- 默认队列 128、事件估算上限 1 MiB；调整后需评估堆占用及 Kafka 消息限制。首版只支持 none/gzip，不打包 Kafka 的本地压缩库。

## 扩展其他外部系统

新增 `doc-listener-spring-boot-starter-xxx` 模块，依赖 `doc-listener-spring-boot-autoconfigure`，实现 `ChangeSink` 并通过自动配置注册 Bean。例如：

```java
@AutoConfiguration(before = DocListenerAutoConfiguration.class)
@ConditionalOnProperty(prefix = "doc-listener.xxx", name = "enabled", havingValue = "true")
public class XxxSinkAutoConfiguration {
    @Bean(destroyMethod = "") // 资源由 dispatcher 按关闭预算统一释放
    public ChangeSink xxxSink() {
        return new XxxChangeSink();
    }
}
```

在该模块的 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 写入配置类全限定名，再把 starter 依赖加进 `doc-listener-es-plugin/pom.xml`。`send` 返回外部确认的 `CompletionStage<Void>`，`close(Duration)` 必须遵守关闭预算，并容忍关闭超时后与尚未返回的 `send` 短暂并发。不要阻塞无限久，不要由 Sink 修改 ES 文档（会造成循环通知），不要引入重复日志实现。

多个 Sink Bean 同时启用时，每个事件都发给所有 Sink。只启用其他系统时可禁用 Kafka；监听启用但没有任何 Sink 会在启动时失败，避免静默丢失。通过 `./mvnw install` 可将模块安装到本机 Maven 仓库，供自己的 starter 工程依赖。

## 验证记录

详见 `docs/verification.md`。单元与上下文测试不等价于真实集群安装；实际多节点、真实 broker、认证及 TLS 需按目标环境验证。
