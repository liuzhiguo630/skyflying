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
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;
import org.apache.skywalking.library.elasticsearch.response.search.SearchResponse;

@Slf4j
@RequiredArgsConstructor
public final class SearchClient {
    private final CompletableFuture<RequestFactory> requestFactory;

    private final WebClient client;

    @SneakyThrows
    public SearchResponse search(Search criteria,
                                 Map<String, Object> params,
                                 String... index) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.search().search(criteria, params, index))
                        .aggregate().thenApply(response -> {
                    if (response.status() != HttpStatus.OK) {
                        throw new RuntimeException(
                            "Failed to search documents: " + Arrays.toString(index) + ". " +
                                response.contentUtf8());
                    }

                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        return rf.codec().decode(is, SearchResponse.class);
                    } catch (Exception e) {
                        log.error("Failed to close input stream", e);
                        return new SearchResponse();
                    }
                }).exceptionally(e -> {
                    log.error("Failed to search documents", e);
                    return new SearchResponse();
                })).get();
    }
}
