/*
 * Copyright (c) 2016 Goran Ehrsson.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gr8crm.autoconfiguration.sequence.redis;

import org.gr8crm.sequence.SequenceGenerator;
import org.gr8crm.sequence.redis.RedisSequenceGenerator;
import org.gr8crm.sequence.redis.SequenceGeneratorRedisProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * AutoConfiguration for SequenceGenerator.
 */
@Configuration
@ComponentScan(basePackages = "org.gr8crm.sequence")
@EnableConfigurationProperties(SequenceGeneratorRedisProperties.class)
public class SequenceGeneratorRedisAutoConfiguration {

    @Autowired
    private SequenceGeneratorRedisProperties config;

    @Bean
    @ConditionalOnMissingBean
    public SequenceGenerator sequenceGenerator() {
        return new RedisSequenceGenerator(jedisPool(), config);
    }

    @Bean
    @ConditionalOnMissingBean
    JedisPool jedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeout(),
                config.getPassword(), config.getDatabase());
    }
}
