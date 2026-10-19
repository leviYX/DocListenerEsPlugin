#!/usr/bin/env bash
# 仅向专用测试索引写入示例数据。ES_USER/ES_PASSWORD 可选；ES_URL 默认 localhost:9200。
set -eo pipefail
# macOS 自带 Bash 3.2 在 set -u 下会将空数组展开误判为未绑定变量。
es_url="${ES_URL:-http://localhost:9200}"
auth=()
if [[ -n "${ES_USER:-}" ]]; then auth=(-u "${ES_USER}:${ES_PASSWORD:?请设置 ES_PASSWORD}"); fi
request() { curl --fail-with-body -sS "${auth[@]}" -H 'Content-Type: application/json' "$@"; echo; }
request -X PUT "$es_url/doc-listener-test" -d '{"settings":{"number_of_shards":1,"number_of_replicas":1}}'
request -X PUT "$es_url/doc-listener-test/_doc/1" -d '{"name":"中文新增","count":1}'
request -X PUT "$es_url/doc-listener-test/_doc/1" -d '{"name":"中文覆盖","count":2}'
request -X POST "$es_url/doc-listener-test/_update/1" -d '{"doc":{"count":3}}'
request -X POST "$es_url/doc-listener-test/_update/1" -d '{"doc":{"count":3}}'
request -X DELETE "$es_url/doc-listener-test/_doc/1"
# 删除不存在的文档会返回 404，因此单独执行，预期不产生事件。
curl -sS "${auth[@]}" -X DELETE "$es_url/doc-listener-test/_doc/1"
echo
printf '%s\n' '{"index":{"_id":"2"}}' '{"name":"批量新增"}' '{"index":{"_id":"3"}}' '{"name":"批量新增2"}' |
  curl --fail-with-body -sS "${auth[@]}" -H 'Content-Type: application/x-ndjson' -X POST "$es_url/doc-listener-test/_bulk" --data-binary @-
echo
# 正常情况下共 6 条事件：3 次写入 + 1 次删除 + bulk 2 次写入。noop、重复删除、副本不计入。
