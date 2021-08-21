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

import org.apache.skywalking.library.elasticsearch.requests.factory.AliasFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.BulkFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.Codec;
import org.apache.skywalking.library.elasticsearch.requests.factory.DocumentFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.IndexFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.SearchFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.TemplateFactory;
import org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec.V6Codec;

public final class V6RequestFactory implements RequestFactory {
    public static final RequestFactory INSTANCE = new V6RequestFactory();

    @Override
    public TemplateFactory template() {
        return V6TemplateFactory.INSTANCE;
    }

    @Override
    public IndexFactory index() {
        return V6IndexFactory.INSTANCE;
    }

    @Override
    public AliasFactory alias() {
        return V6AliasFactory.INSTANCE;
    }

    @Override
    public DocumentFactory document() {
        return V6DocumentFactory.INSTANCE;
    }

    @Override
    public SearchFactory search() {
        return V6SearchFactory.INSTANCE;
    }

    @Override
    public BulkFactory bulk() {
        return V6BulkFactory.INSTANCE;
    }

    @Override
    public Codec codec() {
        return V6Codec.INSTANCE;
    }
}
