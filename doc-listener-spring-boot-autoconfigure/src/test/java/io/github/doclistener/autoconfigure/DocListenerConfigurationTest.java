package io.github.doclistener.autoconfigure;

import io.github.doclistener.core.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;

class DocListenerConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DocListenerAutoConfiguration.class));
    @Test void disabledByDefault() {
        runner.run(c -> assertThat(c).doesNotHaveBean(AsyncDispatcher.class));
    }
    @Test void requiresSinkWhenEnabled() {
        runner.withPropertyValues("doc-listener.enabled=true").run(c -> assertThat(c).hasFailed());
    }
    @Test void acceptsExternalSinkBeanAndBindsProperties() {
        runner.withBean(ChangeSink.class, () -> e -> CompletableFuture.completedFuture(null))
            .withPropertyValues("doc-listener.enabled=true", "doc-listener.queue-capacity=3",
                "doc-listener.include-indices=orders-*,products", "doc-listener.exclude-indices=.*")
            .run(c -> {
                assertThat(c).hasSingleBean(AsyncDispatcher.class);
                var properties = c.getBean(DocListenerProperties.class);
                assertThat(properties.matches("orders-2026")).isTrue();
                assertThat(properties.matches(".security")).isFalse();
                assertThat(properties.matches("orders[1]")).isFalse();
                assertThat(properties.getQueueCapacity()).isEqualTo(3);
            });
    }
    @Test void rejectsInvalidCapacity() {
        runner.withBean(ChangeSink.class, () -> e -> CompletableFuture.completedFuture(null))
            .withPropertyValues("doc-listener.enabled=true", "doc-listener.queue-capacity=0")
            .run(c -> assertThat(c).hasFailed());
    }
    @Test void wildcardTreatsRegexSymbolsLiterally() {
        var properties = new DocListenerProperties();
        properties.setIncludeIndices(java.util.List.of("orders[1]*"));
        assertThat(properties.matches("orders[1]-today")).isTrue();
        assertThat(properties.matches("orders1-today")).isFalse();
    }
}
