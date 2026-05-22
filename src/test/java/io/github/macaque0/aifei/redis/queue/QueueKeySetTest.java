package io.github.macaque0.aifei.redis.queue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueueKeySetTest {

    @Test
    public void keysUseClusterHashTag() {
        QueueKeySet keys = new QueueKeySet("app", "order");

        assertEquals("app:queue:{order}:ready", keys.ready);
        assertEquals("app:queue:{order}:delay", keys.delay);
        assertEquals("app:queue:{order}:reserved", keys.reserved);
        assertEquals("app:queue:{order}:stream", keys.stream);
        assertEquals("app:queue:{order}:control", keys.control);
        assertEquals("app:queue:{order}:priority", keys.priority);
        assertEquals("app:queue:{order}:priority-value", keys.priorityValue);
        assertEquals("app:queue:{order}:dead", keys.dead);
    }

    @Test
    public void keysUseConfiguredDeadLetterSuffix() {
        QueueKeySet keys = new QueueKeySet("app", "order", "failed");

        assertEquals("app:queue:{order}:failed", keys.dead);
    }
}
