package io.github.doclistener.core;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class AsyncDispatcherTest {
    @Test void drainsQueueAndClosesSink() {
        var received = new CopyOnWriteArrayList<DocumentChange>();
        var closed = new AtomicInteger();
        ChangeSink sink = new ChangeSink() {
            public CompletionStage<Void> send(DocumentChange e) { received.add(e); return CompletableFuture.completedFuture(null); }
            public void close(Duration timeout) { closed.incrementAndGet(); }
        };
        var dispatcher = new AsyncDispatcher(List.of(sink), 4, 1048576, Duration.ofSeconds(1));
        assertThat(dispatcher.offer(DocumentChangeTest.event(1))).isTrue();
        assertThat(dispatcher.offer(DocumentChangeTest.event(2))).isTrue();
        dispatcher.close(Duration.ofSeconds(2));
        assertThat(received).extracting(DocumentChange::seqNo).containsExactly(1L, 2L);
        assertThat(dispatcher.stats().sent()).isEqualTo(2);
        assertThat(closed.get()).isEqualTo(1);
        assertThat(dispatcher.offer(DocumentChangeTest.event(3))).isFalse();
        dispatcher.close(Duration.ofMillis(100));
        assertThat(closed.get()).isEqualTo(1);
    }
    @Test void dropsWhenQueueIsFullWithoutBlockingWriteThread() throws Exception {
        var entered = new CountDownLatch(1);
        var pending = new CompletableFuture<Void>();
        ChangeSink sink = e -> { entered.countDown(); return pending; };
        var dispatcher = new AsyncDispatcher(List.of(sink), 1, 1048576, Duration.ofSeconds(5));
        try {
            assertThat(dispatcher.offer(DocumentChangeTest.event(1))).isTrue();
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(dispatcher.offer(DocumentChangeTest.event(2))).isTrue();
            assertThat(dispatcher.offer(DocumentChangeTest.event(3))).isFalse();
            assertThat(dispatcher.stats().dropped()).isEqualTo(1);
        } finally { pending.complete(null); dispatcher.close(Duration.ofSeconds(2)); }
    }
    @Test void isolatesFailedSinkFromOtherSinks() {
        ChangeSink failed = e -> CompletableFuture.failedFuture(new IllegalStateException("模拟错误"));
        var received = new AtomicInteger();
        ChangeSink healthy = e -> { received.incrementAndGet(); return CompletableFuture.completedFuture(null); };
        var dispatcher = new AsyncDispatcher(List.of(failed, healthy), 4, 1048576, Duration.ofSeconds(1));
        dispatcher.offer(DocumentChangeTest.event(1));
        dispatcher.offer(DocumentChangeTest.event(2));
        dispatcher.close(Duration.ofSeconds(2));
        assertThat(received.get()).isEqualTo(2);
        assertThat(dispatcher.stats().failed()).isEqualTo(2);
    }
    @Test void rejectsOversizedEvents() {
        var dispatcher = new AsyncDispatcher(List.of(e -> CompletableFuture.completedFuture(null)), 1, 1, Duration.ofSeconds(1));
        assertThat(dispatcher.offer(DocumentChangeTest.event(1))).isFalse();
        dispatcher.close(Duration.ofSeconds(1));
        assertThat(dispatcher.stats().dropped()).isEqualTo(1);
    }
    @Test void boundedShutdownInterruptsInFlightAndCountsAbandonedEvents() throws Exception {
        var entered = new CountDownLatch(1);
        var dispatcher = new AsyncDispatcher(List.of(e -> { entered.countDown(); return new CompletableFuture<>(); }),
            4, 1048576, Duration.ofSeconds(60));
        dispatcher.offer(DocumentChangeTest.event(1));
        assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
        dispatcher.offer(DocumentChangeTest.event(2));
        long start = System.nanoTime();
        dispatcher.close(Duration.ofMillis(100));
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
        assertThat(dispatcher.stats().dropped()).isEqualTo(1);
    }
}
