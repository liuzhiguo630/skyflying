package org.apache.skywalking.apm.plugin.config.server.apollo;

import org.apache.skywalking.apm.agent.core.boot.PluginConfig;
import org.apache.skywalking.apm.agent.core.conf.Config;

public class ApolloConfiguration {

    public static class Plugin {
        @PluginConfig(root = ApolloConfiguration.class)
        public static class Apollo {
            // Apollo 接入地址，必填，否则报错
            public static String META;

            // appId，默认为 Agent 的 Service Name，也可以独立设置，推荐与 Service Name 保持一致。
            public static String APP_ID = Config.Agent.SERVICE_NAME;

            // apollo namespace. 独立出 namespace 来方便管理。建议创建出公共配置，每个项目只需在个性化某些配置时引入该 namespace。
            public static String NAMESPACE = "skyflying";

            // apollo cluster。
            public static String CLUSTER = "default";
        }
    }
}
