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

package org.apache.skywalking.library.elasticsearch.bulk;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.Exceptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.library.elasticsearch.requests.IndexRequest;
import org.apache.skywalking.library.elasticsearch.requests.UpdateRequest;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;

import static java.util.Objects.requireNonNull;

@Slf4j
public final class BulkProcessor {
    private final ArrayBlockingQueue<Object> requests;

    private final CompletableFuture<RequestFactory> requestFactory;
    private final WebClient client;

    private final int bulkActions;
    private final int concurrentRequests;

    public BulkProcessor(
        final CompletableFuture<RequestFactory> requestFactory,
        final WebClient client, final int bulkActions,
        final Duration flushInterval, final int concurrentRequests) {
        this.requestFactory = requireNonNull(requestFactory, "requestFactory");
        this.client = requireNonNull(client, "client");
        this.bulkActions = bulkActions;

        requireNonNull(flushInterval, "flushInterval");

        this.concurrentRequests = concurrentRequests;
        this.requests = new ArrayBlockingQueue<>(bulkActions + 1);
        Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r);
            thread.setName("ElasticSearch BulkProcessor");
            return thread;
        }).scheduleWithFixedDelay(
            this::flush,
            0, flushInterval.getSeconds(),
            TimeUnit.SECONDS
        );
    }

    public BulkProcessor add(IndexRequest request) {
        internalAdd(request);
        return this;
    }

    public BulkProcessor add(UpdateRequest request) {
        internalAdd(request);
        return this;
    }

    private void internalAdd(Object request) {
        requireNonNull(request, "request");
        requests.add(request);
        flushIfNeeded();
    }

    @SneakyThrows
    private void flushIfNeeded() {
        if (requests.size() >= bulkActions) {
            flush();
        }
    }

    private void flush() {
        if (this.requests.isEmpty()) {
            return;
        }

        final List<Object> requests = new ArrayList<>(this.requests.size());
        this.requests.drainTo(requests);

        log.debug("Executing bulk with {} requests", requests.size());

        requestFactory.thenCompose(rf -> {
            try {
                final List<ByteBuf> bs = new ArrayList<>();
                for (final Object request : requests) {
                    bs.add(rf.codec().encode(request));
                    bs.add(Unpooled.wrappedBuffer("\n".getBytes()));
                }
                final ByteBuf content = Unpooled.wrappedBuffer(bs.toArray(new ByteBuf[0]));
                return client.execute(rf.bulk().bulk(content))
                             .aggregate().thenAccept(response -> {
                        final HttpStatus status = response.status();
                        if (status != HttpStatus.OK) {
                            throw new RuntimeException(
                                "Failed to process bulk request: " + status);
                        }
                    });
            } catch (Exception e) {
                return Exceptions.throwUnsafely(e);
            }
        }).exceptionally(e -> {
            log.error("Failed to execute bulk", e);
            return null;
        }).join();
    }
}
