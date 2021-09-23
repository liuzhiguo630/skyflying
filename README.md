JoJo SkyFlying
==========
本项目基于 Apache SkyWalking 二次开发，100% 兼容 SkyWalking。旨在提供一个更轻量级更便捷好用的 APM 系统。

SkyWalking 项目地址 [SkyWalking](https://github.com/apache/skywalking)

# 简介
SkyFlying 是成都书声科技技术团队，基于 SkyWalking 演进的真正大规模可用的 APM 系统。我们解决了大量实践中发现的问题，并融入了我们对 APM 系统的理解。

# 优势
大部分公司在使用 SkyWalking 时，止步在了预发环境，上线之后出问题也匆匆下线。原因和问题很多不一一列举，不过我总结只有一点，就是本身这个产品并不是生产可用的。
另一种情况是，花了很多预算在 SkyWalking 上，最后却没有收获多少价值。SkyWalking 本身并没有深入挖掘过采集数据的价值。

究其原因，SkyWalking 的贡献者中，没有真正多少人在现实生活中大规模运维过这套系统，所以在很多问题上想当然，或者压根意识不到存在问题。

而 SkyFlying 在我们的系统中经过了长期大规模实践的检验，稳定运行近一年，运维上千节点，每日链路数据量超过 10T。


# Features
## Java Agent
1. 所有参数可动态下发，实时生效。可接入 Apollo 配置中心，修改配置不用再到处修改文件重新分发，或者大量修改环境变量。
2. 更灵活的采样数。支持多通道采样，可按端点类型，错误/慢链路分别设定采样数，确保低采样数下不漏掉重点数据。
3. 后置采样，采样数之外的链路仍然可以被追踪。避免程序中取不到 traceId，并使得链路传递信息变得更加可靠。
4. 可序列化的链路上下文。可指定对应的链路进行继承，使两条没有关系的链路产生联系，方便在链路上观察。
5. 端点监控。通过 Backend 的分析会严重受采样数的影响导致数据失真，我们采用端上的分析，得到了大量极有价值的数据。
6. 跨线程池自动链路关联。无需手动指定 RunnableWrapper。
7. 支持 MySQL 端点采集执行结果数量。方便排查问题。
8. 支持 SQL 级别的统计，而不仅限于慢 SQL。可用于精准排查 SQL 性能问题。
9. 支持自定义 TraceId

## OAP-Server
1. 禁用掉大量无用的分析程序，大幅度降低 ElasticSearch 开销。
2. 链路索引 Segment 支持按小时拆分，大幅提高写入速度。
3. 支持多数据源，链路数据到 ElasticSearch，统计数据到 InfluxDB。

# 使用说明
## Java Agent
### 使用 Apollo 作为配置中心
1. 将 `/optional-plugins/config-server-apollo-plugin-8.7.0.jar` 放入 `plugins` 目录下
2. 指定 apollo 接入地址: 支持直接通过启动参数`-Dapollo.meta=apolloMetaUrl`，或通过参数`-Dskywalking.plugin.apollo.namespace=apolloMetaUrl`. 推荐前者。 
3. 指定 appId: 支持通过启动参数 `-Dapp.id=appId`, 或使用 skywalking 本身的 Agent Name(启动时将环境变量或启动参数中添加 `-DSW_AGENT_NAME=xApp`)
4. 指定 namespace: 默认为 `skyflying`。需要提前在 Apollo 后台创建名为 `skyflying` 的公共 Namespace（不需要每个服务都引入 namespace 即可共享配置）。可通过启动参数 `-Dskywalking.plugin.apollo.namespace=newSkyflyingNamespace` 自定义 namespace。
5. 启动后验证，查看 skywalking-api.log 中是否提示 `load config from apollo succeed`。

Apollo 中的配置是通过 HTTP 定时拉取（每分钟）的，所以配置生效会存在延迟。在 Apollo 中的配置格式，与 `agent.config` 保持一致，具体配置见[Setup java agent](`https://skywalking.apache.org/docs/main/v8.7.0/en/setup/service-agent/java-agent/readme/#table-of-agent-configuration-properties`)

### 不兼容的配置
1. `trace.ignore_path` 改为 `agent.trace_ignore_config`。配置内容格式不变，见 [Support custom trace ignore](https://skywalking.apache.org/docs/main/v8.7.0/en/setup/service-agent/java-agent/agent-optional-plugins/trace-ignore-plugin/)

### 使用 Kafka 作为 Reporter（推荐）
Kafka 比默认的 gRPC 协议 Reporter 高效可靠得多。SkyFlying 基于一些历史经验，对 Kafka 做了动态配置变更，即发现 bootstrap 或 producerConfig 变更后，会创建新的 producer 并销毁旧的。producerConfig 推荐配置如下：
1. `plugin.kafka.producer_config[max.block.ms] = 1000`
2. `plugin.kafka.producer_config[linger.ms]=1000`
3. `plugin.kafka.producer_config[batch.size]=512000`