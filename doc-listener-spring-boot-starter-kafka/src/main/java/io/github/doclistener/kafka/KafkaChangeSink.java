// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.kafka;

import io.github.doclistener.core.ChangeSink;
import io.github.doclistener.core.DocumentChange;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** 后台发送 JSON 消息，完成信号对应 Kafka producer 的异步确认结果。 */
public final class KafkaChangeSink implements ChangeSink {
    private final Producer<String, String> producer;
    private final String topic;

    public KafkaChangeSink(Producer<String, String> producer, String topic) {
        this.producer = Objects.requireNonNull(producer);
        this.topic = Objects.requireNonNull(topic);
    }

    @Override
    public CompletionStage<Void> send(DocumentChange event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            producer.send(new ProducerRecord<>(topic, event.key(), event.toJson()), (metadata, exception) -> {
                if (exception == null) result.complete(null);
                else result.completeExceptionally(exception);
            });
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override public void close(Duration timeout) { producer.close(timeout); }
}
