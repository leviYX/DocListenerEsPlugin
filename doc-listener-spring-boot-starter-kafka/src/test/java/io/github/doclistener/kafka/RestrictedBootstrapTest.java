package io.github.doclistener.kafka;

import io.github.doclistener.autoconfigure.DocListenerBootstrap;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** 在子 JVM 重现 ES 禁止 accessSystemModules 的限制，不影响其他测试线程。 */
@SuppressWarnings("removal")
class RestrictedBootstrapTest {
    @Test void startsWithoutSystemModuleAccess() throws Exception {
        Path output = Files.createTempFile("doc-listener-security", ".log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djava.security.manager=allow", "-cp", System.getProperty("java.class.path"), Probe.class.getName())
            .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(process.waitFor(20, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).withFailMessage(Files.readString(output)).isZero();
        } finally { process.destroyForcibly(); Files.deleteIfExists(output); }
    }

    public static class Probe {
        public static void main(String[] args) throws Exception {
            Path config = Files.createTempFile("doc-listener", ".properties");
            Files.writeString(config, "doc-listener.enabled=false\n");
            // ES 在安装 SecurityManager 前已经初始化自己的 Log4j2。
            org.apache.logging.log4j.LogManager.getLogger(Probe.class);
            System.setSecurityManager(new SecurityManager() {
                @Override public void checkPermission(Permission permission) {
                    if (permission instanceof RuntimePermission && permission.getName().equals("accessSystemModules")) {
                        throw new SecurityException("ES 插件禁止访问系统模块");
                    }
                }
            });
            try (var context = DocListenerBootstrap.open(config)) {
                if (context.isActive() == false) throw new IllegalStateException("上下文未启动");
            } finally { Files.delete(config); }
        }
    }
}
