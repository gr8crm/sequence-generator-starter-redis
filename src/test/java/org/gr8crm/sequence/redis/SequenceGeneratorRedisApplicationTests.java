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

package org.gr8crm.sequence.redis;

import org.gr8crm.sequence.SequenceConfiguration;
import org.gr8crm.sequence.SequenceGenerator;
import org.gr8crm.sequence.SimpleSequenceGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SequenceGeneratorRedisApplicationTests {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SequenceGenerator sequenceGenerator;

    @Test
    public void contextLoads() {
        assertNotNull(context);
        assertNotNull(sequenceGenerator);
        assertTrue(sequenceGenerator instanceof RedisSequenceGenerator);
    }

    @Test
    public void nonExistingSequence() {
        try {
            sequenceGenerator.nextNumber("test", 0, "notfound", "foo");
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("No such sequence: test/0/notfound/foo", e.getMessage());
        }
    }

    @Test
    public void singleSequence() {
        // given:
        SequenceConfiguration config = SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("single")
                .withStart(1)
                .build();

        // when:
        sequenceGenerator.create(config);

        // then:
        assertEquals("1", sequenceGenerator.nextNumber("test", 1, "single", null));
        assertEquals("2", sequenceGenerator.nextNumber("test", 1, "single", null));
        assertEquals("3", sequenceGenerator.nextNumber("test", 1, "single", null));
    }

    @Test
    public void groupedSequence() {
        // given:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("grouped")
                .withGroup("A")
                .withStart(1)
                .build());

        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("grouped")
                .withGroup("B")
                .withStart(1)
                .build());
        // when/then:
        try {
            sequenceGenerator.nextNumber("test", 1, "grouped", "C");
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("No such sequence: test/1/grouped/C", e.getMessage());
        }

        // and:
        assertEquals("1", sequenceGenerator.nextNumber("test", 1, "grouped", "A"));
        assertEquals("2", sequenceGenerator.nextNumber("test", 1, "grouped", "A"));
        assertEquals("3", sequenceGenerator.nextNumber("test", 1, "grouped", "A"));

        // and:
        assertEquals("1", sequenceGenerator.nextNumber("test", 1, "grouped", "B"));
        assertEquals("2", sequenceGenerator.nextNumber("test", 1, "grouped", "B"));
        assertEquals("3", sequenceGenerator.nextNumber("test", 1, "grouped", "B"));

        // and:
        assertEquals("4", sequenceGenerator.nextNumber("test", 1, "grouped", "A"));
        assertEquals("4", sequenceGenerator.nextNumber("test", 1, "grouped", "B"));
    }

    @Test
    public void multiTenantSequences() {
        // given:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("tenant")
                .withStart(1)
                .build());

        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(2)
                .withName("tenant")
                .withStart(1)
                .build());

        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(3)
                .withName("tenant")
                .withStart(1)
                .build());

        // then:
        assertEquals("1", sequenceGenerator.nextNumber("test", 1, "tenant", null));
        assertEquals("2", sequenceGenerator.nextNumber("test", 1, "tenant", null));
        assertEquals("3", sequenceGenerator.nextNumber("test", 1, "tenant", null));

        // and:
        assertEquals("1", sequenceGenerator.nextNumber("test", 2, "tenant", null));
        assertEquals("2", sequenceGenerator.nextNumber("test", 2, "tenant", null));
        assertEquals("3", sequenceGenerator.nextNumber("test", 2, "tenant", null));

        // and:
        assertEquals("1", sequenceGenerator.nextNumber("test", 3, "tenant", null));
        assertEquals("2", sequenceGenerator.nextNumber("test", 3, "tenant", null));
        assertEquals("3", sequenceGenerator.nextNumber("test", 3, "tenant", null));

        // and:
        assertEquals("4", sequenceGenerator.nextNumber("test", 1, "tenant", null));
        assertEquals("4", sequenceGenerator.nextNumber("test", 2, "tenant", null));
        assertEquals("4", sequenceGenerator.nextNumber("test", 3, "tenant", null));
    }

    @Test
    public void leftPaddedFormat() {
        // when:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("pad")
                .withStart(1)
                .withFormat("%04d")
                .build());

        // then:
        assertEquals("0001", sequenceGenerator.nextNumber("test", 1, "pad", null));
        assertEquals("0002", sequenceGenerator.nextNumber("test", 1, "pad", null));
        assertEquals("0003", sequenceGenerator.nextNumber("test", 1, "pad", null));
    }

    @Test
    public void startAt0() {
        // when:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("zero")
                .withStart(0)
                .build());

        // then:
        assertEquals("0", sequenceGenerator.nextNumber("test", 1, "zero", null));
        assertEquals("1", sequenceGenerator.nextNumber("test", 1, "zero", null));
        assertEquals("2", sequenceGenerator.nextNumber("test", 1, "zero", null));
    }

    @Test
    public void startAt1000() {
        // when:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("K")
                .withStart(1000)
                .build());

        // then:
        assertEquals("1000", sequenceGenerator.nextNumber("test", 1, "K", null));
        assertEquals("1001", sequenceGenerator.nextNumber("test", 1, "K", null));
        assertEquals("1002", sequenceGenerator.nextNumber("test", 1, "K", null));
    }

    @Test
    public void mizedRawAndFormattedNumbers() {
        // when:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("mix")
                .withStart(10000)
                .build());

        // then:
        assertEquals(10000, sequenceGenerator.nextNumberLong("test", 1, "mix", null));
        assertEquals("10001", sequenceGenerator.nextNumber("test", 1, "mix", null));
        assertEquals(10002, sequenceGenerator.nextNumberLong("test", 1, "mix", null));
        assertEquals("10003", sequenceGenerator.nextNumber("test", 1, "mix", null));
    }

    @Test
    public void zeroIncrement() {
        // expect:
        try {
            sequenceGenerator.create(SequenceConfiguration.builder()
                    .withApp("test")
                    .withTenant(1)
                    .withName("stuck")
                    .withStart(1)
                    .withIncrement(0)
                    .build());
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("increment must be non-zero", e.getMessage());
        }
    }

    @Test
    public void performance() {
        // given:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("perf")
                .build());

        // when:
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            sequenceGenerator.nextNumber("test", 1, "perf", null);
        }
        System.out.println("100000 calls in " + (System.currentTimeMillis() - startTime) + " ms");

        // then:
        assertEquals(10000, sequenceGenerator.status("test", 1, "perf", null).getNumber());
    }

    @Test
    public void multiThreading() throws Exception {
        // given:
        sequenceGenerator.create(SequenceConfiguration.builder()
                .withApp("test")
                .withTenant(1)
                .withName("threads")
                .build());
        int cores = Runtime.getRuntime().availableProcessors();
        int numberOfThreads = cores * 4;
        int numberOfRequests = 10000;
        final Set<String> numbers = ConcurrentHashMap.newKeySet();
        final List<Thread> threads = new ArrayList<>(numberOfThreads);

        System.out.println("Using " + numberOfThreads + " threads to do " + numberOfRequests + " requests each...");

        // when:
        for (int i = 0; i < numberOfThreads; i++) {
            Thread t = new Thread(() -> {
                for (int n = 0; n < numberOfRequests; n++) {
                    numbers.add(sequenceGenerator.nextNumber("test", 1, "threads", null));
                }
            });
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < numberOfThreads; i++) {
            threads.get(i).join();
        }

        assertEquals(numberOfThreads * numberOfRequests, numbers.size());
    }
}
