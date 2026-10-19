// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.autoconfigure;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.MutablePropertySources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** 只启动轻量 Spring 上下文，不运行 SpringApplication、不接管 ES 日志或进程。 */
public final class DocListenerBootstrap {
    private DocListenerBootstrap() {}

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    public static class StarterConfiguration {}

    public static AnnotationConfigApplicationContext open(Path configFile) throws IOException {
        Properties properties = new Properties();
        if (Files.exists(configFile)) {
            try (var reader = Files.newBufferedReader(configFile, StandardCharsets.UTF_8)) { properties.load(reader); }
        }
        // 禁止业务配置覆盖进程级 JMX 开关，避免向 ES 平台 MBeanServer 注册额外组件。
        properties.setProperty("spring.jmx.enabled", "false");
        var context = new AnnotationConfigApplicationContext();
        context.setClassLoader(DocListenerBootstrap.class.getClassLoader());
        var environment = new StandardEnvironment() {
            @Override protected void customizePropertySources(MutablePropertySources sources) {
                // 配置仅来自指定文件，不扫描整个节点的系统属性和环境变量。
            }
        };
        environment.getPropertySources().addFirst(new PropertiesPropertySource("docListenerFile", properties));
        context.setEnvironment(environment);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DocListenerBootstrap.class.getClassLoader());
            context.register(StarterConfiguration.class);
            context.refresh();
            return context;
        } catch (RuntimeException | Error e) {
            context.close();
            throw e;
        } finally { Thread.currentThread().setContextClassLoader(previous); }
    }
}
