// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 有界、尽力投递的异步派发器。监听线程不执行外部系统 I/O，也不等待空闲队列位置。 */
public final class AsyncDispatcher {
    private static final Logger LOG = LogManager.getLogger(AsyncDispatcher.class);
    private final List<ChangeSink> sinks;
    private final ArrayBlockingQueue<DocumentChange> queue;
    private final int maxEventBytes;
    private final long sendTimeoutMillis;
    private final Object lifecycleLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean accepting = true;
    private final Thread worker;
    private final AtomicLong accepted = new AtomicLong(), sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong(), dropped = new AtomicLong();

    /** accepted/dropped 按事件计数；sent/failed 按接收系统的投递尝试计数。 */
    public record Stats(long accepted, long sent, long failed, long dropped, int queued) {}

    public AsyncDispatcher(List<ChangeSink> sinks, int capacity, int maxEventBytes, Duration sendTimeout) {
        this.sinks = List.copyOf(sinks);
        if (sinks.isEmpty() || capacity <= 0 || maxEventBytes <= 0 || sendTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("接收系统不能为空，队列、事件大小及发送超时必须为正数");
        }
        queue = new ArrayBlockingQueue<>(capacity);
        this.maxEventBytes = maxEventBytes;
        sendTimeoutMillis = sendTimeout.toMillis();
        worker = new Thread(this::run, "doc-listener-dispatcher");
        worker.setDaemon(true);
        worker.start();
    }

    public boolean offer(DocumentChange event) {
        Objects.requireNonNull(event);
        synchronized (lifecycleLock) {
            if (accepting && event.estimatedBytes() <= maxEventBytes && queue.offer(event)) {
                accepted.incrementAndGet();
                return true;
            }
        }
        long count = dropped.incrementAndGet();
        if (isLogPoint(count)) LOG.warn("doc-listener 丢弃事件，累计 dropped={}（队列满、事件过大或正在关闭）", count);
        return false;
    }

    public Stats stats() { return new Stats(accepted.get(), sent.get(), failed.get(), dropped.get(), queue.size()); }

    private void run() {
        long reportAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (accepting || queue.isEmpty() == false) {
            try {
                DocumentChange event = queue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    for (ChangeSink sink : sinks) {
                        try {
                            sink.send(event).toCompletableFuture().get(sendTimeoutMillis, TimeUnit.MILLISECONDS);
                            sent.incrementAndGet();
                        } catch (InterruptedException e) {
                            failed.incrementAndGet();
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Exception e) {
                            long count = failed.incrementAndGet();
                            // 不打印文档、配置和异常详情，避免业务内容或认证凭据进入日志。
                            if (isLogPoint(count)) LOG.warn("doc-listener 投递失败，sink={}，累计 failed={}，异常类型={}",
                                sink.getClass().getSimpleName(), count, e.getClass().getSimpleName());
                        }
                    }
                }
                if (System.nanoTime() >= reportAt) {
                    LOG.info("doc-listener stats={}", stats());
                    reportAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static boolean isLogPoint(long count) { return (count & (count - 1)) == 0; }

    /** 先停止接收，再限时排空；超时未发出的队列事件计入 dropped。 */
    public void close(Duration timeout) {
        if (closed.compareAndSet(false, true) == false) return;
        long deadline = System.nanoTime() + Math.max(0, timeout.toNanos());
        synchronized (lifecycleLock) { accepting = false; }
        try {
            long remaining = deadline - System.nanoTime();
            // join(0) 会无限等待，必须显式避开。
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (worker.isAlive()) worker.interrupt();
        dropped.addAndGet(queue.size());
        queue.clear();
        for (ChangeSink sink : sinks) {
            try { sink.close(Duration.ofNanos(Math.max(0, deadline - System.nanoTime()))); }
            catch (Exception e) { LOG.warn("doc-listener 接收系统关闭失败，类型={}", e.getClass().getSimpleName()); }
        }
        LOG.info("doc-listener 已关闭，stats={}", stats());
    }
}
