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
 *
 */

package org.apache.skywalking.apm.agent.core.kafka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.loader.AgentClassLoader;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * Configuring, initializing and holding a KafkaProducer instance for reporters.
 */
@DefaultImplementor
public class KafkaProducerManager implements BootService, Runnable {

    private static final ILog LOGGER = LogManager.getLogger(KafkaProducerManager.class);

    private Set<String> topics = new HashSet<>();
    private List<KafkaConnectionStatusListener> listeners = new ArrayList<>();

    private volatile KafkaProducer<String, Bytes> producer;

    private Map<String, String> producerConfig;

    private String bootstrapServers;

    @Override
    public void prepare() {
    }

    @Override
    public void boot() {
        Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("kafkaProducerInitThread")
        ).scheduleAtFixedRate(new RunnableWithExceptionProtection(
            this,
            t -> LOGGER.error("unexpected exception.", t)
        ), 0, 60, TimeUnit.SECONDS);
    }

    String formatTopicNameThenRegister(String topic) {
        String topicName = StringUtil.isBlank(KafkaReporterPluginConfig.Plugin.Kafka.NAMESPACE) ? topic
            : KafkaReporterPluginConfig.Plugin.Kafka.NAMESPACE + "-" + topic;
        topics.add(topicName);
        return topicName;
    }

    public void addListener(KafkaConnectionStatusListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void run() {
        Thread.currentThread().setContextClassLoader(AgentClassLoader.getDefault());

        if (producer == null || producerConfig != KafkaReporterPluginConfig.Plugin.Kafka.PRODUCER_CONFIG ||
            !bootstrapServers.equals(KafkaReporterPluginConfig.Plugin.Kafka.BOOTSTRAP_SERVERS)) {

            producerConfig = KafkaReporterPluginConfig.Plugin.Kafka.PRODUCER_CONFIG;
            bootstrapServers = KafkaReporterPluginConfig.Plugin.Kafka.BOOTSTRAP_SERVERS;

            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerConfig.forEach(properties::setProperty);

            try (AdminClient adminClient = AdminClient.create(properties)) {
                DescribeTopicsResult topicsResult = adminClient.describeTopics(topics);
                Set<String> notExistTopics = topicsResult.values().entrySet().stream()
                                                         .map(entry -> {
                                                             try {
                                                                 entry.getValue().get(
                                                                     KafkaReporterPluginConfig.Plugin.Kafka.GET_TOPIC_TIMEOUT,
                                                                     TimeUnit.SECONDS
                                                                 );
                                                                 return null;
                                                             } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                                                 LOGGER.error(
                                                                     e, "Get KAFKA topic:{} error.", entry.getKey());
                                                             }
                                                             return entry.getKey();
                                                         })
                                                         .filter(Objects::nonNull)
                                                         .collect(Collectors.toSet());

                if (!notExistTopics.isEmpty()) {
                    LOGGER.warn("kafka topics {} is not exist, connect to kafka cluster abort", notExistTopics);
                    return;
                }

                try {
                    KafkaProducer<String, Bytes> newProducer = new KafkaProducer<>(
                        properties, new StringSerializer(), new BytesSerializer());
                    if (this.producer != null) {
                        producer.flush();
                        producer.close();
                    }
                    this.producer = newProducer;
                } catch (Exception e) {
                    LOGGER.error(
                        e, "connect to kafka cluster '{}' failed",
                        KafkaReporterPluginConfig.Plugin.Kafka.BOOTSTRAP_SERVERS
                    );
                    return;
                }
                // notify listeners to send data if no exception been throw
                notifyListeners(KafkaConnectionStatus.CONNECTED);
            }
        }

    }

    private void notifyListeners(KafkaConnectionStatus status) {
        for (KafkaConnectionStatusListener listener : listeners) {
            listener.onStatusChanged(status);
        }
    }

    /**
     * Get the KafkaProducer instance to send data to Kafka broker.
     */
    public final KafkaProducer<String, Bytes> getProducer() {
        return producer;
    }

    /**
     * make kafka producer init later but before {@link GRPCChannelManager}
     *
     * @return priority value
     */
    @Override
    public int priority() {
        return ServiceManager.INSTANCE.findService(GRPCChannelManager.class).priority() - 1;
    }

    @Override
    public void shutdown() {
        producer.flush();
        producer.close();
    }
}