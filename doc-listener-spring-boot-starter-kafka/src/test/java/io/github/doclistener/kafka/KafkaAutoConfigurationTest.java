package io.github.doclistener.kafka;

import io.github.doclistener.autoconfigure.*;
import io.github.doclistener.core.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;

class KafkaAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DocListenerAutoConfiguration.class, KafkaSinkAutoConfiguration.class));
    @Test void noKafkaProducerWhenDisabled() {
        runner.run(c -> assertThat(c).doesNotHaveBean(KafkaChangeSink.class));
    }
    @Test void enabledKafkaRequiresValidConfiguration() {
        runner.withPropertyValues("doc-listener.enabled=true", "doc-listener.kafka.enabled=true")
            .run(c -> assertThat(c).hasFailed());
    }
    @Test void customKafkaSinkOverridesDefault() {
        runner.withPropertyValues("doc-listener.enabled=true", "doc-listener.kafka.enabled=true")
            .withBean(KafkaChangeSink.class, () -> new KafkaChangeSink(
                new org.apache.kafka.clients.producer.MockProducer<>(true,
                    new org.apache.kafka.common.serialization.StringSerializer(),
                    new org.apache.kafka.common.serialization.StringSerializer()), "custom"))
            .run(c -> assertThat(c).hasSingleBean(KafkaChangeSink.class).hasSingleBean(AsyncDispatcher.class));
    }
    @Test void bootstrapsRealPropertiesFileWithoutSpringApplication() throws Exception {
        var config = java.nio.file.Files.createTempFile("doc-listener", ".properties");
        try {
            java.nio.file.Files.writeString(config, "doc-listener.enabled=false\n");
            try (var context = DocListenerBootstrap.open(config)) {
                assertThat(context.getBean(DocListenerProperties.class).isEnabled()).isFalse();
                assertThat(context.getBeanNamesForType(AsyncDispatcher.class)).isEmpty();
            }
        } finally { java.nio.file.Files.delete(config); }
    }
    @Test void discoversAdditionalStarterAndDispatchesWithoutKafka() throws Exception {
        ExtraSinkConfiguration.received.clear();
        var config = java.nio.file.Files.createTempFile("doc-listener-extra", ".properties");
        try {
            java.nio.file.Files.writeString(config, "doc-listener.enabled=true\n"
                + "doc-listener.extra.enabled=true\ndoc-listener.shutdown-timeout=2s\n");
            try (var context = DocListenerBootstrap.open(config)) {
                assertThat(context.getBeanNamesForType(KafkaChangeSink.class)).isEmpty();
                context.getBean(AsyncDispatcher.class).offer(new DocumentChange("orders", "uuid", 0,
                    "1", null, 2, 1, DocumentChange.Type.INDEX, true, "{}", 1234));
            }
            assertThat(ExtraSinkConfiguration.received).hasSize(1);
        } finally { java.nio.file.Files.deleteIfExists(config); }
    }

    @org.springframework.boot.autoconfigure.AutoConfiguration(before = DocListenerAutoConfiguration.class)
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(prefix = "doc-listener.extra", name = "enabled", havingValue = "true")
    public static class ExtraSinkConfiguration {
        static final java.util.List<DocumentChange> received = new java.util.concurrent.CopyOnWriteArrayList<>();
        @org.springframework.context.annotation.Bean(destroyMethod = "")
        public ChangeSink extraSink() {
            return e -> { received.add(e); return CompletableFuture.completedFuture(null); };
        }
    }
}
