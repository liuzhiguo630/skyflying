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

package org.apache.skywalking.library.elasticsearch.requests.factory;

import org.apache.skywalking.library.elasticsearch.ElasticSearchVersion;
import org.apache.skywalking.library.elasticsearch.requests.factory.v6.V6RequestFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.skywalking.library.elasticsearch.ElasticSearchVersion.V6_0;
import static org.apache.skywalking.library.elasticsearch.ElasticSearchVersion.V7_0;
import static org.apache.skywalking.library.elasticsearch.ElasticSearchVersion.V8_0;

public interface RequestFactory {
    /**
     * Returns a {@link RequestFactory} that is responsible to compose correct requests according to
     * the syntax of specific {@link ElasticSearchVersion}.
     */
    static RequestFactory of(ElasticSearchVersion version) {
        requireNonNull(version, "version");
        if (version.compareTo(V6_0) >= 0 && version.compareTo(V7_0) < 0) {
            return V6RequestFactory.INSTANCE;
        }
        if (version.compareTo(V7_0) >= 0 && version.compareTo(V8_0) < 0) {
            return V6RequestFactory.INSTANCE;
        }

        throw new UnsupportedOperationException("Version " + version + " is not supported.");
    }

    /**
     * Returns a {@link TemplateFactory} that is dedicated to compose template-related requests.
     *
     * @see TemplateFactory
     */
    TemplateFactory template();

    /**
     * Returns a {@link IndexFactory} that is dedicated to compose index-related requests.
     *
     * @see IndexFactory
     */
    IndexFactory index();

    /**
     * Returns a {@link AliasFactory} that is dedicated to compose alias-related requests.
     *
     * @see AliasFactory
     */
    AliasFactory alias();

    /**
     * Returns a {@link DocumentFactory} that is dedicated to compose document-related requests.
     *
     * @see DocumentFactory
     */
    DocumentFactory document();

    /**
     * Returns a {@link SearchFactory} that is dedicated to compose searching-related requests.
     *
     * @see DocumentFactory
     */
    SearchFactory search();

    /**
     * Returns a {@link SearchFactory} that is dedicated to compose bulk-related requests.
     *
     * @see DocumentFactory
     */
    BulkFactory bulk();

    /**
     * Returns a {@link Codec} to encode the requests and decode the response.
     */
    Codec codec();
}
