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

import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import io.netty.buffer.ByteBuf;
import lombok.SneakyThrows;
import org.apache.skywalking.library.elasticsearch.requests.factory.BulkFactory;

final class V6BulkFactory implements BulkFactory {
    static final BulkFactory INSTANCE = new V6BulkFactory();

    @SneakyThrows
    @Override
    public HttpRequest bulk(ByteBuf content) {
        return HttpRequest.builder()
                          .post("/_bulk")
                          .content(MediaType.JSON, HttpData.wrap(content))
                          .build();
    }
}
