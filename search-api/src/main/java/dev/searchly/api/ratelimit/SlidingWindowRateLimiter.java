package dev.searchly.api.ratelimit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Atomic sliding-window rate limiter backed by a Redis sorted set.
 * Returns true if the request is allowed. See ADR 0007.
 */
@Component
public class SlidingWindowRateLimiter {

    private static final String LUA = """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window_ms = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local member = ARGV[4]
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window_ms)
            local count = redis.call('ZCARD', key)
            if count < limit then
                redis.call('ZADD', key, now, member)
                redis.call('PEXPIRE', key, window_ms)
                return 1
            else
                return 0
            end
            """;

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> script;

    public SlidingWindowRateLimiter(StringRedisTemplate redis) {
        this.redis = redis;
        this.script = new DefaultRedisScript<>(LUA, Long.class);
    }

    public boolean tryAcquire(String key, int limit, long windowMillis) {
        long now = System.currentTimeMillis();
        String member = now + ":" + Math.random();
        Long allowed = redis.execute(script, List.of("rl:" + key),
                String.valueOf(now), String.valueOf(windowMillis),
                String.valueOf(limit), member);
        return allowed != null && allowed == 1L;
    }
}
