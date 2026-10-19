// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.kafka;

import io.github.doclistener.autoconfigure.DocListenerAutoConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** 引入 starter 后自动发现；用户可提供同类型 Bean 替换默认实现。 */
@AutoConfiguration(before = DocListenerAutoConfiguration.class)
@EnableConfigurationProperties(KafkaSinkProperties.class)
@ConditionalOnProperty(prefix = "doc-listener", name = "enabled", havingValue = "true")
public class KafkaSinkAutoConfiguration {
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean(KafkaChangeSink.class)
    @ConditionalOnProperty(prefix = "doc-listener.kafka", name = "enabled", havingValue = "true")
    public KafkaChangeSink kafkaChangeSink(KafkaSinkProperties properties) {
        // 显式提供序列化器，避免错误的配置将事件交给其他类型的序列化器。
        return new KafkaChangeSink(new KafkaProducer<>(properties.producerProperties(),
            new StringSerializer(), new StringSerializer()), properties.getTopic());
    }
}
