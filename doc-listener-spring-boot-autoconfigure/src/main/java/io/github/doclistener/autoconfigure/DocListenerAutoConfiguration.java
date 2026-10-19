// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.autoconfigure;

import io.github.doclistener.core.AsyncDispatcher;
import io.github.doclistener.core.ChangeSink;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import java.util.List;

/** 所有 starter 共用的派发器配置，支持同时向多个 ChangeSink 发送。 */
@AutoConfiguration
@EnableConfigurationProperties(DocListenerProperties.class)
public class DocListenerAutoConfiguration {
    @Bean(destroyMethod = "")
    @ConditionalOnProperty(prefix = "doc-listener", name = "enabled", havingValue = "true")
    public AsyncDispatcher docListenerDispatcher(DocListenerProperties properties, List<ChangeSink> sinks) {
        properties.validate();
        return new AsyncDispatcher(sinks, properties.getQueueCapacity(), properties.getMaxEventBytes(), properties.getSendTimeout());
    }

    /** 单独注册销毁回调，确保先排空队列再释放 Sink；不使用 JVM shutdown hook。 */
    @Bean
    @ConditionalOnProperty(prefix = "doc-listener", name = "enabled", havingValue = "true")
    public DisposableBean docListenerShutdown(AsyncDispatcher dispatcher, DocListenerProperties properties) {
        return () -> dispatcher.close(properties.getShutdownTimeout());
    }
}
