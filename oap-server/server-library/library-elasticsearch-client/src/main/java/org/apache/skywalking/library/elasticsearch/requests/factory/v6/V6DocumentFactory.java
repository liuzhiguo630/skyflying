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

package org.apache.skywalking.library.elasticsearch.requests.factory.v6;

import com.google.common.collect.ImmutableMap;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpRequestBuilder;
import com.linecorp.armeria.common.MediaType;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.skywalking.library.elasticsearch.requests.IndexRequest;
import org.apache.skywalking.library.elasticsearch.requests.UpdateRequest;
import org.apache.skywalking.library.elasticsearch.requests.factory.DocumentFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec.V6Codec;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.requireNonNull;

final class V6DocumentFactory implements DocumentFactory {
    static final DocumentFactory INSTANCE = new V6DocumentFactory();

    @Override
    public HttpRequest exist(String index, String type, String id) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");

        return HttpRequest.builder()
                          .head("/{index}/{type}/{id}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .pathParam("id", id)
                          .build();
    }

    @Override
    public HttpRequest get(String index, String type, String id) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");

        return HttpRequest.builder()
                          .get("/{index}/{type}/{id}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .pathParam("id", id)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest mget(String index, String type, Iterable<String> ids) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(ids != null && !isEmpty(ids), "ids cannot be null or empty");

        final Map<String, Iterable<String>> m = ImmutableMap.of("ids", ids);
        final byte[] content = V6Codec.MAPPER.writeValueAsBytes(m);
        return HttpRequest.builder()
                          .get("/{index}/{type}/_mget")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .content(MediaType.JSON, content)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest index(IndexRequest request, Map<String, Object> params) {
        requireNonNull(request, "request");

        final String index = request.getIndex();
        final String type = request.getType();
        final String id = request.getId();
        final Map<String, Object> doc = request.getDoc();

        checkArgument(!isNullOrEmpty(index), "request.index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "request.type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "request.id cannot be null or empty");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }
        final byte[] content = V6Codec.MAPPER.writeValueAsBytes(doc);

        builder.put("/{index}/{type}/{id}")
               .pathParam("index", index)
               .pathParam("type", type)
               .pathParam("id", id)
               .content(MediaType.JSON, content);

        return builder.build();
    }

    @SneakyThrows
    @Override
    public HttpRequest update(UpdateRequest request, Map<String, Object> params) {
        requireNonNull(request, "request");

        final String index = request.getIndex();
        final String type = request.getType();
        final String id = request.getId();
        final Map<String, Object> doc = request.getDoc();

        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");
        checkArgument(doc != null && !isEmpty(doc.entrySet()), "doc cannot be null or empty");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }
        final byte[] content = V6Codec.MAPPER.writeValueAsBytes(doc);

        builder.put("/{index}/{type}/{id}")
               .pathParam("index", index)
               .pathParam("type", type)
               .pathParam("id", id)
               .content(MediaType.JSON, content);

        return builder.build();
    }

    @Override
    public HttpRequest delete(String index, String type, String query,
                              Map<String, Object> params) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(query), "query cannot be null or empty");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }

        return builder.delete("/{index}/{type}/_delete_by_query")
                      .pathParam("index", index)
                      .pathParam("type", type)
                      .build();
    }
}
