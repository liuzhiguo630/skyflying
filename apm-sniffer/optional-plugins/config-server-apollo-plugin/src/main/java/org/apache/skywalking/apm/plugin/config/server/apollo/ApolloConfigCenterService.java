package org.apache.skywalking.apm.plugin.config.server.apollo;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.OverrideImplementor;
import org.apache.skywalking.apm.agent.core.conf.ConfigCenterService;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

@OverrideImplementor(ConfigCenterService.class)
public class ApolloConfigCenterService extends ConfigCenterService {
    private static final ILog LOGGER = LogManager.getLogger(ApolloConfigCenterService.class);

    private static final Gson GSON = new Gson();

    @Override
    public void prepare() {
        loadAndOverride();

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("SkywalkingApolloConfigExecutor"));

        service.scheduleAtFixedRate(new RunnableWithExceptionProtection(
            this::loadAndOverride, t -> LOGGER.error("unexpected exception.", t)), 1, 1, TimeUnit.MINUTES);

    }

    private void loadAndOverride() {
        Properties properties = loadProperties();
        if (properties != null) {
            overrideConfigs(properties);
        }
        LOGGER.info("load config from apollo succeed");
    }

    private Properties loadProperties() {
        try {
            String apolloMeta = System.getProperty("apollo.meta", ApolloConfiguration.Plugin.Apollo.META);
            if (StringUtil.isEmpty(apolloMeta)) {
                throw new IllegalArgumentException("apollo.meta can not be empty" + apolloMeta);
            }

            String appId = System.getProperty("app.id", ApolloConfiguration.Plugin.Apollo.APP_ID);
            if (StringUtil.isEmpty(appId)) {
                throw new IllegalArgumentException("apollo.appId can not be empty" + appId);
            }

            String services = httpGet(apolloMeta + "/services/config");
            if (StringUtil.isEmpty(services)) {
                throw new RuntimeException("获取 services 列表失败，response:" + services);
            }

            List<ServiceConfig> serviceConfigs = GSON.fromJson(
                services, new TypeToken<List<ServiceConfig>>() {
                }.getType());

            String homepageUrl = null;
            ServiceConfig serviceConfig = serviceConfigs.get(0);
            if (serviceConfig.getAppName().equals("APOLLO-CONFIGSERVICE")) {
                homepageUrl = serviceConfig.getHomepageUrl();
            }
            if (homepageUrl == null) {
                throw new RuntimeException("获取 homepageUrl 列表失败，response:" + serviceConfig);
            }

            String getConfigUrl = String.format("%sconfigs/%s/%s/%s", homepageUrl, appId,
                                                ApolloConfiguration.Plugin.Apollo.CLUSTER,
                                                ApolloConfiguration.Plugin.Apollo.NAMESPACE
            );

            String configResponse = httpGet(getConfigUrl);
            JsonObject configJson = GSON.fromJson(configResponse, JsonObject.class);
            if (configJson == null || configJson.isJsonNull()) {
                throw new RuntimeException("获取配置列表失败，response:" + configResponse);
            }
            JsonObject configurations = configJson.getAsJsonObject("configurations");
            if (configurations == null || configurations.isJsonNull()) {
                return null;
            }
            Properties properties = new Properties();
            for (String key : configurations.keySet()) {
                properties.setProperty(key, configurations.getAsJsonPrimitive(key).getAsString());
            }
            return properties;
        } catch (Exception e) {
            LOGGER.error("加载 apollo 配置失败", e);
        }
        return null;
    }

    public static String httpGet(String httpUrl) throws MalformedURLException {
        HttpURLConnection connection = null;
        InputStream inputStream = null;
        BufferedReader br = null;
        URL url = new URL(httpUrl);
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();
            if (connection.getResponseCode() == 200) {
                inputStream = connection.getInputStream();
                br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                StringBuilder stringBuilder = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    stringBuilder.append(line);
                }
                return stringBuilder.toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
        return null;
    }

    @Override
    public void boot() {

    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {

    }

    private static class ServiceConfig {
        private String appName;
        private String homepageUrl;

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getHomepageUrl() {
            return homepageUrl;
        }

        public void setHomepageUrl(String homepageUrl) {
            this.homepageUrl = homepageUrl;
        }
    }

}
