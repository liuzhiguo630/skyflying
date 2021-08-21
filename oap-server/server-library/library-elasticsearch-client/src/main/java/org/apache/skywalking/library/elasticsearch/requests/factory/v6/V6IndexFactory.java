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

import com.google.common.base.Strings;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.skywalking.library.elasticsearch.requests.factory.IndexFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec.V6Codec;

import static com.google.common.base.Preconditions.checkArgument;

final class V6IndexFactory implements IndexFactory {
    static final IndexFactory INSTANCE = new V6IndexFactory();

    @Override
    public HttpRequest exists(String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .head("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @Override
    public HttpRequest get(final String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .get("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest create(String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .put("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @Override
    public HttpRequest delete(String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .delete("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    public HttpRequest putMapping(String index, String type,
                                  Map<String, Object> mapping) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!Strings.isNullOrEmpty(type), "type cannot be null or empty");

        final Map<String, Object> properties = (Map<String, Object>) mapping.get(type);
        final byte[] content = V6Codec.MAPPER.writeValueAsBytes(properties);
        return HttpRequest.builder()
                          .put("/{index}/_mapping/{type}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .content(MediaType.JSON, content)
                          .build();
    }
}
