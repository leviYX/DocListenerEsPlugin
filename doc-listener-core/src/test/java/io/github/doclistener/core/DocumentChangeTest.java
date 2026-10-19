package io.github.doclistener.core;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentChangeTest {
    static DocumentChange event(long seqNo) {
        return new DocumentChange("订单", "uuid", 2, "a:b", "用户", seqNo, 7,
            DocumentChange.Type.INDEX, true, "{\"金额\":9007199254740993,\"备注\":\"你好\"}", 1234);
    }
    @Test void serializesSourceAsObjectWithoutLosingNumbers() {
        var json = JsonParser.parseString(event(3).toJson()).getAsJsonObject();
        assertThat(json.getAsJsonObject("source").get("金额").getAsString()).isEqualTo("9007199254740993");
        assertThat(json.get("eventId").getAsString()).isEqualTo("uuid:2:7:3");
        assertThat(json.get("type").getAsString()).isEqualTo("INDEX");
        assertThat(json.get("routing").getAsString()).isEqualTo("用户");
    }
    @Test void identifiesDifferentPrimaryTermsSeparately() {
        var a = event(3);
        var b = new DocumentChange(a.index(), a.indexUuid(), a.shardId(), a.id(), a.routing(), 3, 8,
            a.type(), a.created(), a.source(), a.timestamp());
        assertThat(a.eventId()).isNotEqualTo(b.eventId());
        assertThat(a.key()).isEqualTo(b.key());
    }
    @Test void deleteHasNoSource() {
        var event = new DocumentChange("orders", "uuid", 0, "1", null, 5, 1,
            DocumentChange.Type.DELETE, false, null, 1234);
        assertThat(JsonParser.parseString(event.toJson()).getAsJsonObject().get("source").isJsonNull()).isTrue();
    }
}
