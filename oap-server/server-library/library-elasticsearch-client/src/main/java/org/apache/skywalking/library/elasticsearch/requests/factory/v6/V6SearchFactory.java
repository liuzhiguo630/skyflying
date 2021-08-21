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

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpRequestBuilder;
import com.linecorp.armeria.common.MediaType;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.skywalking.library.elasticsearch.requests.factory.SearchFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec.V6Codec;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;

final class V6SearchFactory implements SearchFactory {
    static final SearchFactory INSTANCE = new V6SearchFactory();

    @SneakyThrows
    @Override
    public HttpRequest search(Search search,
                              Map<String, Object> queryParams,
                              String... indices) {
        final HttpRequestBuilder builder = HttpRequest.builder();

        if (indices == null || indices.length == 0) {
            builder.get("/_search");
        } else {
            builder.get("/{indices}/_search")
                   .pathParam("indices", String.join(",", indices));
        }

        if (queryParams != null && !queryParams.isEmpty()) {
            queryParams.forEach(builder::queryParam);
        }

        final byte[] content = V6Codec.MAPPER.writeValueAsBytes(search);

        return builder.content(MediaType.JSON, content)
                      .build();
    }
}
