// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.es;

import io.github.doclistener.core.DocumentChange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** 只在成功主分片操作后生成快照；不监听副本、恢复、noop 或整索引删除。 */
public final class DocumentChangeListener implements IndexingOperationListener {
    private static final Logger LOG = LogManager.getLogger(DocumentChangeListener.class);
    private final Predicate<String> indexFilter;
    private final Consumer<DocumentChange> consumer;
    private final AtomicLong snapshotFailures = new AtomicLong();

    public DocumentChangeListener(Predicate<String> indexFilter, Consumer<DocumentChange> consumer) {
        this.indexFilter = indexFilter;
        this.consumer = consumer;
    }

    @Override
    public void postIndex(ShardId shard, Engine.Index operation, Engine.IndexResult result) {
        if (operation.origin() != Engine.Operation.Origin.PRIMARY
            || result.getResultType() != Engine.Result.Type.SUCCESS || indexFilter.test(shard.getIndexName()) == false) return;
        try {
            // 引擎输入 operation 上的 seqNo 可能仍未分配，必须取引擎结果中的实际序号与任期。
            // 转为字符串快照，不能把引擎持有的可复用 BytesReference 交给异步线程。
            String source = XContentHelper.convertToJson(operation.source(), false, operation.parsedDoc().getXContentType());
            consumer.accept(new DocumentChange(shard.getIndexName(), shard.getIndex().getUUID(), shard.id(),
                operation.id(), operation.routing(), result.getSeqNo(), result.getTerm(), DocumentChange.Type.INDEX,
                result.isCreated(), source, System.currentTimeMillis()));
        } catch (Exception e) { snapshotFailed(e); }
    }

    @Override
    public void postDelete(ShardId shard, Engine.Delete operation, Engine.DeleteResult result) {
        if (operation.origin() != Engine.Operation.Origin.PRIMARY || result.getResultType() != Engine.Result.Type.SUCCESS
            || result.isFound() == false || indexFilter.test(shard.getIndexName()) == false) return;
        try {
            // Engine.Delete 不携带 routing 和旧 source，事件中明确使用 null，不额外查询文档。
            consumer.accept(new DocumentChange(shard.getIndexName(), shard.getIndex().getUUID(), shard.id(),
                operation.id(), null, result.getSeqNo(), result.getTerm(), DocumentChange.Type.DELETE,
                false, null, System.currentTimeMillis()));
        } catch (Exception e) { snapshotFailed(e); }
    }

    private void snapshotFailed(Exception e) {
        long count = snapshotFailures.incrementAndGet();
        if ((count & (count - 1)) == 0) LOG.warn("doc-listener 事件快照失败，累计={}，异常类型={}", count, e.getClass().getSimpleName());
    }

    public long snapshotFailures() { return snapshotFailures.get(); }
}
