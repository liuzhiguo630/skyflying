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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.WebClientBuilder;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.auth.BasicToken;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.library.elasticsearch.bulk.BulkProcessorBuilder;
import org.apache.skywalking.library.elasticsearch.client.AliasClient;
import org.apache.skywalking.library.elasticsearch.client.DocumentClient;
import org.apache.skywalking.library.elasticsearch.client.IndexClient;
import org.apache.skywalking.library.elasticsearch.client.SearchClient;
import org.apache.skywalking.library.elasticsearch.client.TemplateClient;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;
import org.apache.skywalking.library.elasticsearch.response.NodeInfo;
import org.apache.skywalking.library.elasticsearch.response.search.SearchResponse;

@Slf4j
public final class ElasticSearchClient implements AutoCloseable {
    private final ObjectMapper mapper = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final WebClient client;

    private final ClientFactory clientFactory;

    private final CompletableFuture<RequestFactory> requestFactory;

    private final TemplateClient templateClient;

    private final IndexClient indexClient;

    private final DocumentClient documentClient;

    private final AliasClient aliasClient;

    private final SearchClient searchClient;

    ElasticSearchClient(SessionProtocol protocol,
                        String username, String password,
                        EndpointGroup endpointGroup,
                        ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        this.requestFactory = new CompletableFuture<>();

        final WebClientBuilder builder = WebClient.builder(protocol, endpointGroup);
        if (StringUtil.isNotBlank(username) && StringUtil.isNotBlank(password)) {
            builder.auth(BasicToken.of(username, password));
        }
        builder.factory(clientFactory);
        client = builder.build();

        templateClient = new TemplateClient(requestFactory, client);
        documentClient = new DocumentClient(requestFactory, client);
        indexClient = new IndexClient(requestFactory, client);
        aliasClient = new AliasClient(requestFactory, client);
        searchClient = new SearchClient(requestFactory, client);
    }

    public static ElasticSearchClientBuilder builder() {
        return new ElasticSearchClientBuilder();
    }

    public void connect() {
        final CompletableFuture<Void> future =
            client.get("/").aggregate().thenAcceptAsync(response -> {
                final HttpStatus status = response.status();
                if (status != HttpStatus.OK) {
                    throw new RuntimeException(
                        "Failed to connect to ElasticSearch server: " + status);
                }
                try (final HttpData content = response.content();
                     final InputStream is = content.toInputStream()) {
                    final NodeInfo node = mapper.readValue(is, NodeInfo.class);
                    final String v = node.getVersion().getNumber();
                    final ElasticSearchVersion version = ElasticSearchVersion.from(v);
                    requestFactory.complete(RequestFactory.of(version));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        future.exceptionally(throwable -> {
            log.error("Failed to determine ElasticSearch version", throwable);
            requestFactory.completeExceptionally(throwable);
            return null;
        });
        future.join();
    }

    public TemplateClient templates() {
        return templateClient;
    }

    public DocumentClient documents() {
        return documentClient;
    }

    public IndexClient index() {
        return indexClient;
    }

    public AliasClient alias() {
        return aliasClient;
    }

    public SearchResponse search(Search search, Map<String, Object> params, String... index) {
        return searchClient.search(search, params, index);
    }

    public SearchResponse search(Search search, String... index) {
        return search(search, null, index);
    }

    public BulkProcessorBuilder bulkProcessor() {
        return new BulkProcessorBuilder(requestFactory, client);
    }

    @Override
    public void close() {
        clientFactory.close();
    }
}
