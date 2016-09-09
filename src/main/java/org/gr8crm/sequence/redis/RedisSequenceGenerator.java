package org.gr8crm.sequence.redis;

import org.gr8crm.sequence.SequenceGenerator;
import org.gr8crm.sequence.SequenceInitializer;
import org.gr8crm.sequence.SequenceStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by goran on 2016-08-20.
 */
@Component
@Primary
public class RedisSequenceGenerator implements SequenceGenerator {

    private static final String KEY_SEPARATOR = "/";
    private static final String KEY_SUFFIX_COUNTER = "";
    private static final String KEY_SUFFIX_INCREMENT = "+";
    private static final String KEY_SUFFIX_FORMAT = "%";
    private static final String DEFAULT_FORMAT = "%d";
    private static final int DEFAULT_INCREMENT = 1;

    private final SequenceGeneratorRedisProperties properties;

    private final SequenceInitializer sequenceInitializer;

    private final JedisPool jedisPool;

    @Autowired
    public RedisSequenceGenerator(JedisPool jedisPool, SequenceInitializer sequenceInitializer,
                                  SequenceGeneratorRedisProperties properties) {
        this.jedisPool = jedisPool;
        this.sequenceInitializer = sequenceInitializer;
        this.properties = properties;
    }

    private String key(long tenant, String name, String group, String suffix) {
        List<String> tmp = new ArrayList<>(4);

        tmp.add(String.valueOf(tenant));

        if (name != null) {
            tmp.add(name);
        }
        if (group != null) {
            tmp.add(group);
        }
        if (suffix != null) {
            tmp.add(suffix);
        }
        return String.join(KEY_SEPARATOR, tmp);
    }

    private String getFormat(long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            String format = jedis.get(key(tenant, name, group, KEY_SUFFIX_FORMAT));
            if (format == null) {
                format = jedis.get(key(tenant, name, null, KEY_SUFFIX_FORMAT));
                if (format == null) {
                    format = DEFAULT_FORMAT;
                }
            }
            return format;
        }
    }

    private int getIncrement(long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key(tenant, name, group, KEY_SUFFIX_INCREMENT));
            if (value == null) {
                value = jedis.get(key(tenant, name, null, KEY_SUFFIX_INCREMENT));
                if (value == null) {
                    return DEFAULT_INCREMENT;
                }
            }
            return Integer.valueOf(value);
        }
    }

    @Override
    public SequenceStatus create(long tenant, String name, String group) {
        final SequenceStatus status = sequenceInitializer.initialize(tenant, name, group);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key(tenant, name, group, KEY_SUFFIX_FORMAT), status.getFormat());
            jedis.set(key(tenant, name, group, KEY_SUFFIX_INCREMENT), String.valueOf(status.getIncrement()));
            jedis.set(key(tenant, name, group, KEY_SUFFIX_COUNTER), String.valueOf(status.getNumber() - status.getIncrement()));
        }
        return status;
    }

    @Override
    public boolean delete(long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (jedis.get(key(tenant, name, group, KEY_SUFFIX_COUNTER)) != null) {
                jedis.set(key(tenant, name, group, KEY_SUFFIX_FORMAT), null);
                jedis.set(key(tenant, name, group, KEY_SUFFIX_COUNTER), null);
                jedis.set(key(tenant, name, group, KEY_SUFFIX_INCREMENT), null);
                return true;
            }
            return false;
        }
    }

    @Override
    public String nextNumber(long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            String format = jedis.get(key(tenant, name, group, KEY_SUFFIX_FORMAT));
            if (format == null) {
                format = jedis.get(key(tenant, name, null, KEY_SUFFIX_FORMAT));
                if (format == null) {
                    format = DEFAULT_FORMAT;
                }
            }
            String value = jedis.get(key(tenant, name, group, KEY_SUFFIX_INCREMENT));
            int increment;
            if (value == null) {
                value = jedis.get(key(tenant, name, null, KEY_SUFFIX_INCREMENT));
                increment = value != null ? Integer.valueOf(value) : DEFAULT_INCREMENT;
            } else {
                increment = Integer.valueOf(value);
            }
            final String key = key(tenant, name, group, KEY_SUFFIX_COUNTER);
            if (jedis.get(key) != null) {
                Long number = jedis.incrBy(key, increment);
                return String.format(format, number);
            } else {
                throw new IllegalArgumentException("No such sequence: " + key(tenant, name, group, null));
            }
        }
    }

    @Override
    public long nextNumberLong(long tenant, String name, String group) {
        final String key = key(tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key(tenant, name, group, KEY_SUFFIX_INCREMENT));
            int increment;
            if (value == null) {
                value = jedis.get(key(tenant, name, null, KEY_SUFFIX_INCREMENT));
                increment = value != null ? Integer.valueOf(value) : DEFAULT_INCREMENT;
            } else {
                increment = Integer.valueOf(value);
            }
            if (jedis.get(key) != null) {
                return jedis.incrBy(key(tenant, name, group, KEY_SUFFIX_COUNTER), increment);
            } else {
                throw new IllegalArgumentException("No such sequence: " + key(tenant, name, group, null));
            }
        }
    }

    @Override
    public SequenceStatus update(long tenant, String name, String group, String format, long current, long start, int increment) {
        final String key = key(tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            final String stringValue = jedis.get(key);
            if (stringValue == null) {
                throw new IllegalArgumentException("No such sequence: " + key(tenant, name, group, null));
            }
            long longValue = Long.parseLong(stringValue) + increment;
            if (longValue == current) {
                jedis.set(key, String.valueOf(start - increment));
            }
            return new SequenceStatus(name, group, format, Long.parseLong(jedis.get(key)) + increment, increment);
        }
    }

    @Override
    public SequenceStatus status(long tenant, String name, String group) {
        final String key = key(tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            String format = jedis.get(key(tenant, name, group, KEY_SUFFIX_FORMAT));
            if (format == null) {
                format = jedis.get(key(tenant, name, null, KEY_SUFFIX_FORMAT));
                if (format == null) {
                    format = DEFAULT_FORMAT;
                }
            }
            String value = jedis.get(key(tenant, name, group, KEY_SUFFIX_INCREMENT));
            int increment;
            if (value == null) {
                value = jedis.get(key(tenant, name, null, KEY_SUFFIX_INCREMENT));
                increment = value != null ? Integer.valueOf(value) : DEFAULT_INCREMENT;
            } else {
                increment = Integer.valueOf(value);
            }
            String stringValue = jedis.get(key);
            if (stringValue == null) {
                throw new IllegalArgumentException("No such sequence: " + key(tenant, name, group, null));
            }
            long longValue = Long.parseLong(stringValue);
            return new SequenceStatus(name, group, format, longValue + increment, increment);
        }
    }

    @Override
    public Stream<SequenceStatus> statistics(long l) {
        return null; // TODO implement me!
    }

    @Override
    public void shutdown() {

    }
}
