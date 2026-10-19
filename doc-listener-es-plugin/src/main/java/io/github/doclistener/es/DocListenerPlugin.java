// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.es;

import io.github.doclistener.autoconfigure.DocListenerBootstrap;
import io.github.doclistener.autoconfigure.DocListenerProperties;
import io.github.doclistener.core.AsyncDispatcher;
import io.github.doclistener.core.DocumentChange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.List;

/** 节点插件入口；每个节点只创建一个上下文和一个派发器，全部索引共享。 */
@SuppressWarnings("removal")
public final class DocListenerPlugin extends Plugin {
    private static final Logger LOG = LogManager.getLogger(DocListenerPlugin.class);
    private final Path configFile;
    private final RuntimeLifecycle runtime = new RuntimeLifecycle();
    private final DocumentChangeListener listener = new DocumentChangeListener(this::matches, this::publish);
    private volatile AnnotationConfigApplicationContext context;
    private volatile DocListenerProperties properties;
    private volatile AsyncDispatcher dispatcher;

    /** ES 8.16.1 使用 Settings、Path 构造参数，Path 为该节点的配置目录。 */
    public DocListenerPlugin(Settings settings, Path configPath) {
        configFile = configPath.resolve("doc-listener.properties");
    }

    @Override public Collection<?> createComponents(PluginServices services) { return List.of(runtime); }

    @Override public void onIndexModule(IndexModule module) { module.addIndexOperationListener(listener); }

    private boolean matches(String index) {
        var current = properties;
        return dispatcher != null && current != null && current.isEnabled() && current.matches(index);
    }

    private void publish(DocumentChange event) {
        var current = dispatcher;
        if (current != null) current.offer(event);
    }

    /** ES 回调栈包含不具备插件权限的服务端代码，特权边界只覆盖插件自身的启动/关闭。 */
    private static void privileged(Runnable action) {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> { action.run(); return null; });
    }

    private final class RuntimeLifecycle extends AbstractLifecycleComponent {
        @Override protected void doStart() {
            privileged(() -> {
                try {
                    context = DocListenerBootstrap.open(configFile);
                    properties = context.getBean(DocListenerProperties.class);
                    dispatcher = properties.isEnabled() ? context.getBean(AsyncDispatcher.class) : null;
                    LOG.info("doc-listener 已启动，enabled={}，配置文件={}", properties.isEnabled(), configFile);
                } catch (IOException e) { throw new IllegalStateException("无法读取 doc-listener.properties", e); }
            });
        }

        @Override protected void doStop() { shutdown(); }
        @Override protected void doClose() { shutdown(); }
    }

    private synchronized void shutdown() {
        dispatcher = null;
        var current = context;
        context = null;
        if (current != null) privileged(current::close);
    }

    @Override public void close() { shutdown(); }
}
