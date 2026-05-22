package io.github.macaque0.aifei.redis.queue;

import java.util.Arrays;
import java.util.List;

class QueueKeySet {

    final String name;
    final String ready;
    final String delay;
    final String reserved;
    final String payload;
    final String meta;
    final String attempts;
    final String dead;
    final String stream;
    final String control;
    final String priority;
    final String priorityValue;

    QueueKeySet(String prefix, String name) {
        this(prefix, name, "dead");
    }

    QueueKeySet(String prefix, String name, String deadSuffix) {
        this.name = requireName(name);
        String base = queueBase(trimToNull(prefix), this.name);
        this.ready = base + ":ready";
        this.delay = base + ":delay";
        this.reserved = base + ":reserved";
        this.payload = base + ":payload";
        this.meta = base + ":meta";
        this.attempts = base + ":attempts";
        this.dead = base + ":" + trimToDefault(deadSuffix, "dead");
        this.stream = base + ":stream";
        this.control = base + ":control";
        this.priority = base + ":priority";
        this.priorityValue = base + ":priority-value";
    }

    List<String> commonKeys() {
        return Arrays.asList(ready, reserved, payload, meta, attempts, delay, dead);
    }

    private static String queueBase(String prefix, String name) {
        String namespace = prefix == null ? "aifei" : prefix;
        return namespace + ":queue:{" + name + "}";
    }

    private static String requireName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("queue name can not be blank");
        }
        return name.trim();
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static String trimToDefault(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}
