package io.github.doclistener.es;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DocListenerPluginTest {
    @TempDir Path config;
    @Test void registersListenerAndUsesManagedLifecycle() throws Exception {
        Files.writeString(config.resolve("doc-listener.properties"), "doc-listener.enabled=false\n");
        var plugin = new DocListenerPlugin(Settings.EMPTY, config);
        var components = plugin.createComponents(null);
        assertThat(components).hasSize(1);
        var lifecycle = (LifecycleComponent) components.iterator().next();
        var module = mock(IndexModule.class);
        plugin.onIndexModule(module);
        verify(module).addIndexOperationListener(any(DocumentChangeListener.class));
        lifecycle.start(); lifecycle.stop(); lifecycle.close(); plugin.close();
    }
    @Test void missingConfigStartsDisabled() throws Exception {
        var plugin = new DocListenerPlugin(Settings.EMPTY, config);
        var lifecycle = (LifecycleComponent) plugin.createComponents(null).iterator().next();
        lifecycle.start(); lifecycle.stop(); lifecycle.close();
    }
}
