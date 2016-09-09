package org.gr8crm.sequence.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by goran on 2016-09-08.
 */
@ConfigurationProperties(
    prefix = "sequence-generator.redis"
)
public class SequenceGeneratorRedisProperties {
/*
    private Redis redis;

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }

    public static class Redis {*/
        private String host = "localhost";
        private int port = 6379;
        private int timeout = 30000;
        private int database;
        private String password;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public int getDatabase() {
            return database;
        }

        public void setDatabase(int database) {
            this.database = database;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    //}
}
