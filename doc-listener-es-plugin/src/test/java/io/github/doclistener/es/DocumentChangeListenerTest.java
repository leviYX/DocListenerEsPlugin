package io.github.doclistener.es;

import io.github.doclistener.core.DocumentChange;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DocumentChangeListenerTest {
    private final ArrayList<DocumentChange> events = new ArrayList<>();
    private final DocumentChangeListener listener = new DocumentChangeListener(name -> name.startsWith("orders"), events::add);
    private final ShardId shard = new ShardId("orders", "uuid", 2);
    private Engine.Index index(Engine.Operation.Origin origin) {
        var operation = mock(Engine.Index.class);
        var parsed = mock(ParsedDocument.class);
        when(operation.origin()).thenReturn(origin);
        when(operation.id()).thenReturn("1"); when(operation.routing()).thenReturn("tenant");
        when(operation.source()).thenReturn(new BytesArray("{\"金额\":123}"));
        when(operation.parsedDoc()).thenReturn(parsed);
        when(parsed.getXContentType()).thenReturn(XContentType.JSON);
        return operation;
    }
    @Test void usesAssignedResultSequenceNumberAndTermForPrimaryWrites() {
        listener.postIndex(shard, index(Engine.Operation.Origin.PRIMARY), new Engine.IndexResult(1, 7, 10, true, "1"));
        assertThat(events).hasSize(1);
        assertThat(events.get(0).seqNo()).isEqualTo(10);
        assertThat(events.get(0).primaryTerm()).isEqualTo(7);
        assertThat(events.get(0).source()).isEqualTo("{\"金额\":123}");
        assertThat(events.get(0).created()).isTrue();
        assertThat(events.get(0).routing()).isEqualTo("tenant");
    }
    @Test void ignoresReplicasRecoveryFailuresAndExcludedIndices() {
        for (var origin : Engine.Operation.Origin.values()) {
            if (origin != Engine.Operation.Origin.PRIMARY) {
                listener.postIndex(shard, index(origin), new Engine.IndexResult(1, 7, 10, true, "1"));
            }
        }
        listener.postIndex(shard, index(Engine.Operation.Origin.PRIMARY), new Engine.IndexResult(new Exception("failed"), 1, "1"));
        listener.postIndex(new ShardId("products", "uuid", 0), index(Engine.Operation.Origin.PRIMARY), new Engine.IndexResult(1, 7, 10, true, "1"));
        assertThat(events).isEmpty();
    }
    @Test void emitsOnlySuccessfulDeletesOfExistingDocuments() {
        var delete = mock(Engine.Delete.class);
        when(delete.origin()).thenReturn(Engine.Operation.Origin.PRIMARY);
        when(delete.id()).thenReturn("1");
        listener.postDelete(shard, delete, new Engine.DeleteResult(2, 7, 11, false, "1"));
        listener.postDelete(shard, delete, new Engine.DeleteResult(new Exception("failed"), 2, 7, "1"));
        listener.postDelete(shard, delete, new Engine.DeleteResult(2, 7, 11, true, "1"));
        assertThat(events).hasSize(1);
        assertThat(events.get(0).type()).isEqualTo(DocumentChange.Type.DELETE);
        assertThat(events.get(0).source()).isNull();
        assertThat(events.get(0).routing()).isNull();
    }
    @Test void sinkSnapshotFailureDoesNotFailTheWrite() {
        var broken = new DocumentChangeListener(n -> true, e -> { throw new IllegalStateException("模拟失败"); });
        assertThatCode(() -> broken.postIndex(shard, index(Engine.Operation.Origin.PRIMARY),
            new Engine.IndexResult(1, 7, 10, true, "1"))).doesNotThrowAnyException();
        assertThat(broken.snapshotFailures()).isEqualTo(1);
    }
}
