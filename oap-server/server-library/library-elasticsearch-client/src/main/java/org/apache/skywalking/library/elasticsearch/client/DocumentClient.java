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

package org.apache.skywalking.library.elasticsearch.client;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.library.elasticsearch.requests.IndexRequest;
import org.apache.skywalking.library.elasticsearch.requests.UpdateRequest;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;
import org.apache.skywalking.library.elasticsearch.response.Document;
import org.apache.skywalking.library.elasticsearch.response.Documents;

@Slf4j
@RequiredArgsConstructor
public final class DocumentClient {
    private final CompletableFuture<RequestFactory> requestFactory;

    private final WebClient client;

    @SneakyThrows
    public boolean exists(String index, String type, String id) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.document().exist(index, type, id))
                        .aggregate().thenApply(response -> response.status() == HttpStatus.OK)
                        .exceptionally(e -> {
                            log.error("Failed to check whether document exists", e);
                            return false;
                        })).get();
    }

    @SneakyThrows
    public Optional<Document> get(String index, String type, String id) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.document().get(index, type, id))
                        .aggregate().thenApply(response -> {
                    if (response.status() != HttpStatus.OK) {
                        return Optional.<Document>empty();
                    }

                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        return Optional.of(rf.codec().decode(is, Document.class));
                    } catch (Exception e) {
                        log.error("Failed to close input stream", e);
                        return Optional.<Document>empty();
                    }
                })).get();
    }

    @SneakyThrows
    public Optional<Documents> mget(String index, String type, Iterable<String> ids) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.document().mget(index, type, ids))
                        .aggregate().thenApply(response -> {
                    if (response.status() != HttpStatus.OK) {
                        return Optional.<Documents>empty();
                    }

                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        return Optional.of(rf.codec().decode(is, Documents.class));
                    } catch (Exception e) {
                        log.error("Failed to close input stream", e);
                        return Optional.<Documents>empty();
                    }
                })).get();
    }

    @SneakyThrows
    public void index(IndexRequest request, Map<String, Object> params) {
        requestFactory.thenCompose(
            rf -> client.execute(rf.document().index(request, params))
                        .aggregate().thenAccept(response -> {
                    final HttpStatus status = response.status();
                    if (status != HttpStatus.CREATED && status != HttpStatus.OK) {
                        throw new RuntimeException("Failed to index doc: " + status);
                    }
                })).join();
    }

    @SneakyThrows
    public void update(UpdateRequest request, Map<String, Object> params) {
        requestFactory.thenCompose(
            rf -> client.execute(rf.document().update(request, params))
                        .aggregate().thenAccept(response -> {
                    final HttpStatus status = response.status();
                    if (status != HttpStatus.OK) {
                        throw new RuntimeException("Failed to update doc: " + status);
                    }
                })).join();
    }

    @SneakyThrows
    public void delete(String index, String type, String query,
                       Map<String, Object> params) {
        requestFactory.thenCompose(
            rf -> client.execute(rf.document().delete(index, type, query, params))
                        .aggregate().thenAccept(response -> {
                    final HttpStatus status = response.status();
                    if (status != HttpStatus.OK) {
                        throw new RuntimeException("Failed to delete docs by query: " + status);
                    }
                })).join();
    }
}
