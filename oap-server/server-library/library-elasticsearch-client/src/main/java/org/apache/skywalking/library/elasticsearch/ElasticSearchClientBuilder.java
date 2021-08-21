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

import com.google.common.collect.ImmutableList;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.client.endpoint.healthcheck.HealthCheckedEndpointGroup;
import com.linecorp.armeria.common.SessionProtocol;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManagerFactory;
import lombok.SneakyThrows;
import org.apache.skywalking.apm.util.StringUtil;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.skywalking.apm.util.StringUtil.isNotBlank;

public final class ElasticSearchClientBuilder {
    private SessionProtocol protocol = SessionProtocol.HTTP;

    private String username;

    private String password;

    private Duration healthCheckRetryInterval = Duration.ofSeconds(30);

    private final ImmutableList.Builder<String> endpoints = ImmutableList.builder();

    private String trustStorePath;

    private String trustStorePass;

    private Duration connectTimeout = Duration.ofMillis(500);

    public ElasticSearchClientBuilder protocol(String protocol) {
        checkArgument(isNotBlank(protocol), "protocol cannot be blank");
        this.protocol = SessionProtocol.of(protocol);
        return this;
    }

    public ElasticSearchClientBuilder username(String username) {
        this.username = requireNonNull(username, "username");
        return this;
    }

    public ElasticSearchClientBuilder password(String password) {
        this.password = requireNonNull(password, "password");
        return this;
    }

    public ElasticSearchClientBuilder endpoints(Iterable<String> endpoints) {
        requireNonNull(endpoints, "endpoints");
        this.endpoints.addAll(endpoints);
        return this;
    }

    public ElasticSearchClientBuilder endpoints(String... endpoints) {
        return endpoints(Arrays.asList(endpoints));
    }

    public ElasticSearchClientBuilder healthCheckRetryInterval(Duration healthCheckRetryInterval) {
        requireNonNull(healthCheckRetryInterval, "healthCheckRetryInterval");
        this.healthCheckRetryInterval = healthCheckRetryInterval;
        return this;
    }

    public ElasticSearchClientBuilder trustStorePath(String trustStorePath) {
        requireNonNull(trustStorePath, "trustStorePath");
        this.trustStorePath = trustStorePath;
        return this;
    }

    public ElasticSearchClientBuilder trustStorePass(String trustStorePass) {
        requireNonNull(trustStorePass, "trustStorePass");
        this.trustStorePass = trustStorePass;
        return this;
    }

    public ElasticSearchClientBuilder connectTimeout(int connectTimeout) {
        checkArgument(connectTimeout > 0, "connectTimeout must be positive");
        this.connectTimeout = Duration.ofMillis(connectTimeout);
        return this;
    }

    @SneakyThrows
    public ElasticSearchClient build() {
        final List<Endpoint> endpoints =
            this.endpoints.build().stream().filter(StringUtil::isNotBlank).map(it -> {
                final String[] parts = it.split(":", 2);
                if (parts.length == 2) {
                    return Endpoint.of(parts[0], Integer.parseInt(parts[1]));
                }
                return Endpoint.of(parts[0]);
            }).collect(Collectors.toList());
        final HealthCheckedEndpointGroup endpointGroup =
            HealthCheckedEndpointGroup.builder(EndpointGroup.of(endpoints), "_cluster/health")
                                      .protocol(protocol)
                                      .useGet(true)
                                      .retryInterval(healthCheckRetryInterval)
                                      .build();
        final ClientFactoryBuilder factoryBuilder = ClientFactory.builder();
        factoryBuilder.connectTimeout(connectTimeout);

        if (StringUtil.isNotBlank(trustStorePath)) {
            final TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            final KeyStore truststore = KeyStore.getInstance("jks");
            try (final InputStream is = Files.newInputStream(Paths.get(trustStorePath))) {
                truststore.load(is, trustStorePass.toCharArray());
            } // TODO: test
            trustManagerFactory.init(truststore);

            factoryBuilder.tlsCustomizer(
                sslContextBuilder -> sslContextBuilder.trustManager(trustManagerFactory));
        }

        return new ElasticSearchClient(
            protocol,
            username,
            password,
            endpointGroup,
            factoryBuilder.build()
        );
    }
}
