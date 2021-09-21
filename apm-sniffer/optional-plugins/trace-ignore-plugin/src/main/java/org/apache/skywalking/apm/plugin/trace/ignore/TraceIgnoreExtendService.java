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

package org.apache.skywalking.apm.plugin.trace.ignore;

import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.OverrideImplementor;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.plugin.trace.ignore.matcher.FastPathMatcher;
import org.apache.skywalking.apm.plugin.trace.ignore.matcher.TracePathMatcher;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@OverrideImplementor(SamplingService.class)
public class TraceIgnoreExtendService extends SamplingService {
    private static final ILog LOGGER = LogManager.getLogger(TraceIgnoreExtendService.class);
    private static final String PATTERN_SEPARATOR = ",";
    private final TracePathMatcher pathMatcher = new FastPathMatcher();
    private String[] patterns = new String[] {};
    private String traceIgnoreConfig = "";

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void boot() {
        loadConfig();

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("TraceIgnoreSamplingService"));

        service.scheduleAtFixedRate(new RunnableWithExceptionProtection(
            this::loadConfig, t -> LOGGER.error("unexpected exception.", t)), 1, 1, TimeUnit.MINUTES);

        super.boot();
    }

    private void loadConfig() {
        if (!Config.Agent.TRACE_IGNORE_CONFIG.equals(traceIgnoreConfig)) {
            traceIgnoreConfig = Config.Agent.TRACE_IGNORE_CONFIG;
            patterns = StringUtil.trim(traceIgnoreConfig, '\'').split(PATTERN_SEPARATOR);
        }
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public boolean trySampling(final String operationName) {
        if (patterns.length > 0) {
            for (String pattern : patterns) {
                if (pathMatcher.match(pattern, operationName)) {
                    LOGGER.debug("operationName : " + operationName + " Ignore tracking");
                    return false;
                }
            }
        }
        return super.trySampling(operationName);
    }

    @Override
    public void forceSampled() {
        super.forceSampled();
    }
}
