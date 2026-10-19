package io.github.doclistener.kafka;

import io.github.doclistener.core.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class KafkaChangeSinkTest {
    private DocumentChange event() {
        return new DocumentChange("orders", "uuid", 0, "1", null, 9, 2,
            DocumentChange.Type.INDEX, true, "{\"name\":\"中文\"}", 1234);
    }
    @Test void sendsJsonAndWaitsForBrokerAcknowledgement() {
        var producer = new MockProducer<String,String>(false, new StringSerializer(), new StringSerializer());
        var sink = new KafkaChangeSink(producer, "changes");
        var future = sink.send(event()).toCompletableFuture();
        assertThat(future.isDone()).isFalse();
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).key()).isEqualTo(event().key());
        assertThat(producer.history().get(0).value()).contains("中文", "\"source\":{");
        producer.completeNext();
        assertThat(future).isCompleted();
        sink.close(Duration.ofSeconds(1));
        assertThat(producer.closed()).isTrue();
    }
    @Test void propagatesAsynchronousBrokerFailure() {
        var producer = new MockProducer<String,String>(false, new StringSerializer(), new StringSerializer());
        var future = new KafkaChangeSink(producer, "changes").send(event()).toCompletableFuture();
        producer.errorNext(new IllegalStateException("broker unavailable"));
        assertThat(future).isCompletedExceptionally();
    }
    @Test void enforcesIdempotenceAndSafeCompression() {
        var properties = new KafkaSinkProperties();
        properties.setBootstrapServers("localhost:9092"); properties.setTopic("changes");
        properties.setProperties(Map.of("enable.idempotence", "false"));
        assertThatThrownBy(properties::producerProperties).isInstanceOf(IllegalArgumentException.class);
        properties.setProperties(Map.of("compression.type", "snappy"));
        assertThatThrownBy(properties::producerProperties).isInstanceOf(IllegalArgumentException.class);
        properties.setProperties(Map.of("compression.type", "gzip"));
        assertThat(properties.producerProperties()).containsEntry("acks", "all").containsEntry("enable.idempotence", "true");
    }
    @Test void requiresTopicAndBrokers() {
        assertThatThrownBy(() -> new KafkaSinkProperties().producerProperties()).isInstanceOf(IllegalArgumentException.class);
    }
}
