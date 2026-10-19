// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Objects;

/** 不可变事件快照；source 是本次写入内容，不包含更新前的旧文档。 */
public record DocumentChange(String index, String indexUuid, int shardId, String id, String routing,
    long seqNo, long primaryTerm, Type type, boolean created, String source, long timestamp) {
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    public enum Type { INDEX, DELETE }

    public DocumentChange {
        Objects.requireNonNull(index); Objects.requireNonNull(indexUuid);
        Objects.requireNonNull(id); Objects.requireNonNull(type);
    }

    /** 任期参与标识，避免故障切换后同一 seqNo 的不同操作被误判为重复。 */
    public String eventId() { return indexUuid + ":" + shardId + ":" + primaryTerm + ":" + seqNo; }

    /** 数组编码避免索引名、文档 ID 含分隔符时出现 key 歧义。 */
    public String key() { return JSON.toJson(new String[] { indexUuid, id }); }

    /** 后台线程执行序列化；保留 source 中的大整数精度和嵌套 JSON 结构。 */
    public String toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", 1); json.addProperty("eventId", eventId());
        json.addProperty("index", index); json.addProperty("indexUuid", indexUuid);
        json.addProperty("shardId", shardId); json.addProperty("id", id);
        json.addProperty("routing", routing); json.addProperty("seqNo", seqNo);
        json.addProperty("primaryTerm", primaryTerm); json.addProperty("type", type.name());
        json.addProperty("created", created); json.addProperty("timestamp", timestamp);
        json.add("source", source == null ? null : JsonParser.parseString(source));
        return JSON.toJson(json);
    }

    /** 按 UTF-16 字符数估计驻留内存上限，额外预留元数据空间。 */
    public long estimatedBytes() {
        return 512L + 2L * (index.length() + indexUuid.length() + id.length()
            + (routing == null ? 0 : routing.length()) + (source == null ? 0 : source.length()));
    }
}
