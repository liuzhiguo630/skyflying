package org.apache.skywalking.apm.agent.core.conf;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.plugin.loader.AgentClassLoader;

/**
 * 第三方配置中心，优先级最高。
 * 继承该类即可，并在取得配置后调用 overrideConfigs() 方法
 */
@DefaultImplementor
public class ConfigCenterService implements BootService {

    @Override
    public void prepare() throws Throwable {
    }

    @Override
    public void boot() throws Throwable {
    }

    @Override
    public void onComplete() throws Throwable {
    }

    @Override
    public void shutdown() throws Throwable {
    }

    /**
     * 更新所有配置，包括插件中的配置
     */
    public void overrideConfigs(Properties properties) {
        for (final Object key : properties.keySet()) {
            String value = properties.getProperty(key.toString(), "");
            try {
                SnifferConfigInitializer.overrideConfigByAgentOptions(key + "=" + value);
            } catch (IllegalArgumentException e) {
                // ignore this
            }
        }

        if (!AgentClassLoader.AGENT_CLASS_LOADER_SET_HASH_MAP.isEmpty()) {
            for (Map.Entry<AgentClassLoader, Set<Class>> agentClassLoaderSetEntry : AgentClassLoader.AGENT_CLASS_LOADER_SET_HASH_MAP.entrySet()) {
                agentClassLoaderSetEntry.getValue().forEach(pluginRootClass -> {
                    SnifferConfigInitializer.initializeConfig(pluginRootClass);
                });
            }
        }

        SnifferConfigInitializer.initializeConfig(Config.class);
    }
}
