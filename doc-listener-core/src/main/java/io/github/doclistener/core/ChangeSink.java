// SPDX-License-Identifier: Apache-2.0
package io.github.doclistener.core;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/** 外部系统扩展点。实现类注册成 Spring Bean 即可参与派发。 */
@FunctionalInterface
public interface ChangeSink {
    /** 返回异步确认结果；不得返回 null。超时后系统可能继续派发其他事件。 */
    CompletionStage<Void> send(DocumentChange event);

    /** 释放资源；实现方必须遵守剩余关闭预算，不得无限阻塞。关闭超时后可能与 send 短暂并发。 */
    default void close(Duration timeout) {}
}
