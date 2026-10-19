// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

/** 节点级配置；修改后需重启该节点，不读取环境变量中的业务配置。 */
@ConfigurationProperties("doc-listener")
public class DocListenerProperties {
    private boolean enabled;
    private int queueCapacity = 128;
    private int maxEventBytes = 1048576;
    private Duration sendTimeout = Duration.ofSeconds(15);
    private Duration shutdownTimeout = Duration.ofSeconds(10);
    private List<String> includeIndices = List.of("*");
    private List<String> excludeIndices = List.of(".*");
    private List<Pattern> includes = compile(includeIndices), excludes = compile(excludeIndices);

    public boolean matches(String index) {
        return includes.stream().anyMatch(p -> p.matcher(index).matches())
            && excludes.stream().noneMatch(p -> p.matcher(index).matches());
    }

    public void validate() {
        if (queueCapacity <= 0 || maxEventBytes <= 0 || includeIndices.isEmpty()
            || sendTimeout == null || sendTimeout.toMillis() <= 0 || shutdownTimeout == null
            || shutdownTimeout.isNegative() || shutdownTimeout.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("doc-listener 队列、大小、超时或索引过滤配置不合法；关闭超时范围 0~5 分钟");
        }
    }

    /** 只支持 * 和 ? 通配符，其他正则字符均按字面值处理。 */
    private static List<Pattern> compile(List<String> globs) {
        return globs.stream().map(glob -> {
            StringBuilder regex = new StringBuilder("^");
            for (char c : glob.toCharArray()) {
                regex.append(c == '*' ? ".*" : c == '?' ? "." : Pattern.quote(String.valueOf(c)));
            }
            return Pattern.compile(regex.append('$').toString());
        }).toList();
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int value) { queueCapacity = value; }
    public int getMaxEventBytes() { return maxEventBytes; }
    public void setMaxEventBytes(int value) { maxEventBytes = value; }
    public Duration getSendTimeout() { return sendTimeout; }
    public void setSendTimeout(Duration value) { sendTimeout = value; }
    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration value) { shutdownTimeout = value; }
    public List<String> getIncludeIndices() { return includeIndices; }
    public void setIncludeIndices(List<String> value) { includeIndices = List.copyOf(value); includes = compile(value); }
    public List<String> getExcludeIndices() { return excludeIndices; }
    public void setExcludeIndices(List<String> value) { excludeIndices = List.copyOf(value); excludes = compile(value); }
}
