#!/usr/bin/env python3
"""检查插件 ZIP 的平铺结构、版本和重复类；可选对比真实 ES lib 目录。"""
import io
import sys
import zipfile
from pathlib import Path

archive = Path(sys.argv[1] if len(sys.argv) > 1 else "doc-listener-es-plugin/target/doc-listener-1.0.0.zip")
classes = {}

def check_jar(name, content):
    with zipfile.ZipFile(content) as jar:
        for cls in jar.namelist():
            if not cls.endswith(".class") or cls == "module-info.class" or cls.startswith("META-INF/"):
                continue
            if cls in classes:
                raise SystemExit(f"重复类: {cls}: {classes[cls]} 和 {name}")
            classes[cls] = name

with zipfile.ZipFile(archive) as plugin:
    names = plugin.namelist()
    descriptor = plugin.read("plugin-descriptor.properties").decode()
    assert "elasticsearch.version=8.16.1" in descriptor
    assert "java.version=17" in descriptor
    assert "${" not in descriptor
    assert "plugin-security.policy" in names
    for name in names:
        assert "/" not in name, f"插件必须平铺: {name}"
        if name.endswith(".jar"):
            assert not name.startswith(("elasticsearch-", "lucene-", "log4j-api-", "log4j-core-")), name
            check_jar(name, io.BytesIO(plugin.read(name)))
if len(sys.argv) > 2:
    for jar in Path(sys.argv[2]).glob("*.jar"):
        check_jar(str(jar), jar)
print(f"ZIP 检查通过: {archive}，已检查 {len(classes)} 个类")
