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

import org.gr8crm.sequence.SequenceGeneratorProperties;
import org.gr8crm.sequence.SequenceGeneratorService;
import org.gr8crm.sequence.SimpleSequenceInitializer;
import org.junit.After;
import org.junit.Before;
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
    private SequenceGeneratorService sequenceGeneratorService;

    @Autowired
    private SequenceGeneratorProperties properties;

    @Before
    public void setup() {
        properties.setDefault(false, "%d", 1, 1);
    }

    @After
    public void cleanup() {
        sequenceGeneratorService.shutdown();
    }

    @Test
    public void contextLoads() {
        assertNotNull(context);
        assertNotNull(sequenceGeneratorService);
        context.getBean(SimpleSequenceInitializer.class);
        context.getBean(RedisSequenceGenerator.class);
    }

    @Test
    public void defaultConfiguration() {
        assertFalse(properties.getDefault().isCreate());
        assertEquals("%d", properties.getDefault().getFormat());
        assertEquals(1, properties.getDefault().getStart());
        assertEquals(1, properties.getDefault().getIncrement());
    }

    @Test
    public void customConfiguration() {
        assertTrue(properties.getSequence("custom").isCreate());
        assertEquals("%04d", properties.getSequence("custom").getFormat());
        assertEquals(1000, properties.getSequence("custom").getStart());
        assertEquals(10, properties.getSequence("custom").getIncrement());
    }

    @Test
    public void nonExistingSequence() {
        try {
            sequenceGeneratorService.nextNumber("notfound", "foo", 0);
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("No such sequence: 0/notfound/foo", e.getMessage());
        }
    }

    @Test
    public void singleSequence() {
        // when:
        sequenceGeneratorService.initialize("single");

        // then:
        assertEquals("1", sequenceGeneratorService.nextNumber("single"));
        assertEquals("2", sequenceGeneratorService.nextNumber("single"));
        assertEquals("3", sequenceGeneratorService.nextNumber("single"));
    }

    @Test
    public void groupedSequence() {
        // given:
        sequenceGeneratorService.initialize("grouped", "A");
        sequenceGeneratorService.initialize("grouped", "B");

        // when/then:
        try {
            sequenceGeneratorService.nextNumber("grouped", "C");
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("No such sequence: 0/grouped/C", e.getMessage());
        }

        // and:
        assertEquals("1", sequenceGeneratorService.nextNumber("grouped", "A"));
        assertEquals("2", sequenceGeneratorService.nextNumber("grouped", "A"));
        assertEquals("3", sequenceGeneratorService.nextNumber("grouped", "A"));

        // and:
        assertEquals("1", sequenceGeneratorService.nextNumber("grouped", "B"));
        assertEquals("2", sequenceGeneratorService.nextNumber("grouped", "B"));
        assertEquals("3", sequenceGeneratorService.nextNumber("grouped", "B"));

        // and:
        assertEquals("4", sequenceGeneratorService.nextNumber("grouped", "A"));
        assertEquals("4", sequenceGeneratorService.nextNumber("grouped", "B"));
    }

    @Test
    public void multiTenantSequences() {
        // when:
        sequenceGeneratorService.initialize("test", "group", 1);
        sequenceGeneratorService.initialize("test", "group", 2);
        sequenceGeneratorService.initialize("test", "group", 3);

        // then:
        assertEquals("1", sequenceGeneratorService.nextNumber("test", "group", 1));
        assertEquals("2", sequenceGeneratorService.nextNumber("test", "group", 1));
        assertEquals("3", sequenceGeneratorService.nextNumber("test", "group", 1));

        // and:
        assertEquals("1", sequenceGeneratorService.nextNumber("test", "group", 2));
        assertEquals("2", sequenceGeneratorService.nextNumber("test", "group", 2));
        assertEquals("3", sequenceGeneratorService.nextNumber("test", "group", 2));

        // and:
        assertEquals("1", sequenceGeneratorService.nextNumber("test", "group", 3));
        assertEquals("2", sequenceGeneratorService.nextNumber("test", "group", 3));
        assertEquals("3", sequenceGeneratorService.nextNumber("test", "group", 3));

        // and:
        assertEquals("4", sequenceGeneratorService.nextNumber("test", "group", 1));
        assertEquals("4", sequenceGeneratorService.nextNumber("test", "group", 2));
        assertEquals("4", sequenceGeneratorService.nextNumber("test", "group", 3));
    }

    @Test
    public void leftPaddedFormat() {
        // given:
        properties.setSequence("test", "pad", false, "%04d", 1, 1);

        // when:
        sequenceGeneratorService.initialize("test", "pad");

        // then:
        assertEquals("0001", sequenceGeneratorService.nextNumber("test", "pad"));
        assertEquals("0002", sequenceGeneratorService.nextNumber("test", "pad"));
        assertEquals("0003", sequenceGeneratorService.nextNumber("test", "pad"));
    }

    @Test
    public void startAt0() {
        // given:
        properties.setSequence("test", "zero", false, "%d", 0, 1);

        // when:
        sequenceGeneratorService.initialize("test", "zero");

        // then:
        assertEquals("0", sequenceGeneratorService.nextNumber("test", "zero"));
        assertEquals("1", sequenceGeneratorService.nextNumber("test", "zero"));
        assertEquals("2", sequenceGeneratorService.nextNumber("test", "zero"));
    }

    @Test
    public void startAt1000() {
        // given:
        properties.setSequence("test", "K", false, "%d", 1000, 1);

        // when:
        sequenceGeneratorService.initialize("test", "K");

        // then:
        assertEquals("1000", sequenceGeneratorService.nextNumber("test", "K"));
        assertEquals("1001", sequenceGeneratorService.nextNumber("test", "K"));
        assertEquals("1002", sequenceGeneratorService.nextNumber("test", "K"));
    }

    @Test
    public void mizedRawAndFormattedNumbers() {
        // given:
        properties.setSequence("test", "mix", false, "%d", 10000, 1);

        // when:
        sequenceGeneratorService.initialize("test", "mix");

        // then:
        assertEquals(10000, sequenceGeneratorService.nextNumberLong("test", "mix"));
        assertEquals("10001", sequenceGeneratorService.nextNumber("test", "mix"));
        assertEquals(10002, sequenceGeneratorService.nextNumberLong("test", "mix"));
        assertEquals("10003", sequenceGeneratorService.nextNumber("test", "mix"));
    }

    @Test
    public void zeroIncrement() {
        // given:
        properties.setSequence("test", "stuck", false, "%d", 1, 0);

        // when:
        try {
            sequenceGeneratorService.initialize("test", "stuck");
            fail("IllegalArgumentException expected here");
        } catch (IllegalArgumentException e) {
            assertEquals("increment must be non-zero", e.getMessage());
        }
    }

    @Test
    public void performance() {
        // given:
        properties.setSequence("test", "perf", false, "%d", 0, 1);
        sequenceGeneratorService.initialize("test", "perf");

        // when:
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            sequenceGeneratorService.nextNumber("test", "perf");
        }
        System.out.println("100000 calls in " + (System.currentTimeMillis() - startTime) + " ms");

        // then:
        assertEquals(10000, sequenceGeneratorService.status("test", "perf").getNumber());
    }

    @Test
    public void multiThreading() throws Exception {
        // given:
        sequenceGeneratorService.initialize("test", "threads");
        int cores = Runtime.getRuntime().availableProcessors();
        int numberOfThreads = cores * 2;
        int numberOfRequests = 10000;
        final Set<String> numbers = ConcurrentHashMap.newKeySet();
        final List<Thread> threads = new ArrayList<>(numberOfThreads);

        System.out.println("Using " + numberOfThreads + " threads that do " + numberOfRequests + " requests each...");

        // when:
        for (int i = 0; i < numberOfThreads; i++) {
            Thread t = new Thread(() -> {
                for (int n = 0; n < numberOfRequests; n++) {
                    numbers.add(sequenceGeneratorService.nextNumber("test", "threads"));
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
