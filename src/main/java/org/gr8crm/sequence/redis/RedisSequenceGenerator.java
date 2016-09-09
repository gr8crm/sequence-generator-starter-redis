package org.gr8crm.sequence.redis;

import org.gr8crm.sequence.SequenceConfiguration;
import org.gr8crm.sequence.SequenceGenerator;
import org.gr8crm.sequence.SequenceStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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


    private final JedisPool jedisPool;

    @Autowired
    public RedisSequenceGenerator(JedisPool jedisPool, SequenceGeneratorRedisProperties properties) {
        this.jedisPool = jedisPool;
        this.properties = properties;
    }

    private String key(SequenceConfiguration config, String suffix) {
        return key(config.getApp(), config.getTenant(), config.getName(), config.getGroup(), suffix);
    }

    private String key(String app, long tenant, String name, String group, String suffix) {
        Objects.requireNonNull(app, "application name must be specified");
        if (app.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("application name cannot contain " + KEY_SEPARATOR);
        }
        List<String> tmp = new ArrayList<>(5);

        tmp.add(app);

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

    private int getIncrement(Jedis jedis, String app, long tenant, String name, String group) {
        String value = jedis.get(key(app, tenant, name, group, KEY_SUFFIX_INCREMENT));
        if (value == null) {
            if (group == null) {
                return DEFAULT_INCREMENT;
            } else {
                value = jedis.get(key(app, tenant, name, null, KEY_SUFFIX_INCREMENT));
                if (value == null) {
                    return DEFAULT_INCREMENT;
                }
            }
        }
        return Integer.valueOf(value);
    }

    private SequenceConfiguration getConfiguration(Jedis jedis, String app, long tenant, String name, String group) {
        SequenceConfiguration.Builder builder = SequenceConfiguration.builder()
                .withApp(app)
                .withTenant(tenant)
                .withName(name)
                .withGroup(group);

        String value = jedis.get(key(app, tenant, name, group, KEY_SUFFIX_INCREMENT));
        if (value == null && group != null) {
            value = jedis.get(key(app, tenant, name, null, KEY_SUFFIX_INCREMENT)); // Try without group
        }
        if (value != null) {
            builder.withIncrement(Integer.valueOf(value));
        }

        value = jedis.get(key(app, tenant, name, group, KEY_SUFFIX_FORMAT));
        if (value == null && group != null) {
            value = jedis.get(key(app, tenant, name, null, KEY_SUFFIX_FORMAT)); // Try without group
        }
        if (value != null) {
            builder.withFormat(value);
        }

        return builder.build();
    }

    @Override
    public SequenceStatus create(final SequenceConfiguration config) {
        final long lastNumber = config.getStart() - config.getIncrement();

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key(config, KEY_SUFFIX_FORMAT), config.getFormat());
            jedis.set(key(config, KEY_SUFFIX_INCREMENT), String.valueOf(config.getIncrement()));
            jedis.set(key(config, KEY_SUFFIX_COUNTER), String.valueOf(lastNumber));
        }
        return new SequenceStatus(config, lastNumber);
    }

    @Override
    public boolean delete(String app, long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (jedis.get(key(app, tenant, name, group, KEY_SUFFIX_COUNTER)) != null) {
                jedis.set(key(app, tenant, name, group, KEY_SUFFIX_FORMAT), null);
                jedis.set(key(app, tenant, name, group, KEY_SUFFIX_COUNTER), null);
                jedis.set(key(app, tenant, name, group, KEY_SUFFIX_INCREMENT), null);
                return true;
            }
            return false;
        }
    }

    @Override
    public String nextNumber(String app, long tenant, String name, String group) {
        try (Jedis jedis = jedisPool.getResource()) {
            String format = jedis.get(key(app, tenant, name, group, KEY_SUFFIX_FORMAT));
            if (format == null) {
                format = jedis.get(key(app, tenant, name, null, KEY_SUFFIX_FORMAT));
                if (format == null) {
                    format = DEFAULT_FORMAT;
                }
            }
            int increment = getIncrement(jedis, app, tenant, name, group);
            final String key = key(app, tenant, name, group, KEY_SUFFIX_COUNTER);
            if (jedis.get(key) != null) {
                Long number = jedis.incrBy(key, increment);
                return String.format(format, number);
            } else {
                throw new IllegalArgumentException("No such sequence: " + key(app, tenant, name, group, null));
            }
        }
    }

    @Override
    public long nextNumberLong(String app, long tenant, String name, String group) {
        final String key = key(app, tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key(app, tenant, name, group, KEY_SUFFIX_INCREMENT));
            int increment;
            if (value == null) {
                value = jedis.get(key(app, tenant, name, null, KEY_SUFFIX_INCREMENT));
                increment = value != null ? Integer.valueOf(value) : DEFAULT_INCREMENT;
            } else {
                increment = Integer.valueOf(value);
            }
            if (jedis.get(key) != null) {
                return jedis.incrBy(key(app, tenant, name, group, KEY_SUFFIX_COUNTER), increment);
            } else {
                throw new IllegalArgumentException("No such sequence: " + key(app, tenant, name, group, null));
            }
        }
    }

    @Override
    public SequenceStatus update(String app, long tenant, String name, String group, long current, long newCurrent) {
        final String key = key(app, tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            final String stringValue = jedis.get(key);
            if (stringValue == null) {
                throw new IllegalArgumentException("No such sequence: " + key(app, tenant, name, group, null));
            }
            SequenceConfiguration config = getConfiguration(jedis, app, tenant, name, group);
            int increment = config.getIncrement();
            long longValue = Long.parseLong(stringValue) + increment;
            if (longValue == current) {
                jedis.set(key, String.valueOf(newCurrent - increment));
            }
            return status(app, tenant, name, group);
        }
    }

    @Override
    public SequenceStatus status(String app, long tenant, String name, String group) {
        final String key = key(app, tenant, name, group, KEY_SUFFIX_COUNTER);
        try (Jedis jedis = jedisPool.getResource()) {
            String stringValue = jedis.get(key);
            if (stringValue == null) {
                throw new IllegalArgumentException("No such sequence: " + key(app, tenant, name, group, null));
            }
            long longValue = Long.parseLong(stringValue);
            SequenceConfiguration config = getConfiguration(jedis, app, tenant, name, group);
            return new SequenceStatus(config, longValue + config.getIncrement());
        }
    }

    @Override
    public Stream<SequenceStatus> statistics(String app, long tenant) {
        return null; // TODO implement me!
    }

    @Override
    public void shutdown() {

    }
}
