// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.LinkedHashMap;
import java.util.Map;

/** Kafka 客户端配置；密码等敏感字段不参与 toString 或插件日志输出。 */
@ConfigurationProperties("doc-listener.kafka")
public class KafkaSinkProperties {
    private boolean enabled;
    private String bootstrapServers;
    private String topic;
    private Map<String, String> properties = new LinkedHashMap<>();

    public Map<String, Object> producerProperties() {
        if (bootstrapServers == null || bootstrapServers.isBlank() || topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("启用 Kafka 时必须设置 bootstrap-servers 和 topic");
        }
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("client.id", "es-doc-listener");
        config.put("acks", "all"); config.put("enable.idempotence", "true");
        config.put("max.in.flight.requests.per.connection", "1");
        config.put("delivery.timeout.ms", "10000"); config.put("request.timeout.ms", "5000");
        config.put("max.block.ms", "1000"); config.put("buffer.memory", "8388608");
        config.put("compression.type", "none");
        config.putAll(properties);
        if ("true".equals(String.valueOf(config.get("enable.idempotence"))) == false
            || "all".equals(config.get("acks")) == false
            || "1".equals(String.valueOf(config.get("max.in.flight.requests.per.connection"))) == false) {
            throw new IllegalArgumentException("Kafka 必须保持 enable.idempotence=true、acks=all 和 max.in.flight.requests.per.connection=1");
        }
        if (java.util.Set.of("none", "gzip").contains(config.get("compression.type")) == false) {
            throw new IllegalArgumentException("首版仅支持 none/gzip 压缩，不加载其他压缩本地库");
        }
        return config;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String value) { bootstrapServers = value; }
    public String getTopic() { return topic; }
    public void setTopic(String value) { topic = value; }
    public Map<String, String> getProperties() { return properties; }
    public void setProperties(Map<String, String> value) { properties = new LinkedHashMap<>(value); }
}
