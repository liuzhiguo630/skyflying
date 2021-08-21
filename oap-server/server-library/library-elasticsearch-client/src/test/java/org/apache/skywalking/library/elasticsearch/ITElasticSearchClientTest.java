/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.library.elasticsearch;

import java.util.Arrays;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.library.elasticsearch.requests.IndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RequiredArgsConstructor
@RunWith(Parameterized.class)
public class ITElasticSearchClientTest {

    @Parameterized.Parameters(name = "version: {0}")
    public static Collection<Object[]> versions() {
        return Arrays.asList(new Object[][] {
            {"6.3.2"} // TODO: add 7.x version
        });
    }

    private final String version;

    private ElasticsearchContainer server;
    private ElasticSearchClient client;

    @Before
    public void setup() {
        server = new ElasticsearchContainer(
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
                           .withTag(version)
        );
        server.start();

        client = ElasticSearchClient.builder()
                                    .endpoints(server.getHttpHostAddress())
                                    .build();
        client.connect();
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @Test
    public void testIndex() {
        final String index = "test-index";
        assertFalse(client.index().exists(index));
        assertTrue(client.index().get(index).isEmpty());
        assertTrue(client.index().create(index));
        assertTrue(client.index().exists(index));
        assertNotNull(client.index().get(index));
        assertTrue(client.index().delete(index));
        assertTrue(client.index().get(index).isEmpty());
    }

    @Test
    public void testDoc() {
        final String index = "test-index";
        assertTrue(client.index().create(index));

        final ImmutableMap<String, Object> doc = ImmutableMap.of("key", "val");
        final String idWithSpace = "an id"; // UI management templates' IDs contains spaces
        final String type = "type";

        client.documents().index(
            IndexRequest.builder()
                        .index(index)
                        .type(type)
                        .id(idWithSpace)
                        .doc(doc)
                        .build(), null);

        assertTrue(client.documents().get(index, type, idWithSpace).isPresent());
        assertEquals(client.documents().get(index, type, idWithSpace).get().getId(), idWithSpace);
        assertEquals(client.documents().get(index, type, idWithSpace).get().getSource(), doc);
    }
}
