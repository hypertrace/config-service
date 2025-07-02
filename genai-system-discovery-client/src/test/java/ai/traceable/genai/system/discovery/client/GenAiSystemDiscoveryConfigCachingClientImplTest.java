package ai.traceable.genai.system.discovery.client;

import static ai.traceable.genai.system.discovery.config.service.v1.Operator.OPERATOR_MATCHES_REGEX;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.genai.system.discovery.config.GenAiSystemDiscoveryConfigCachingClientConfig;
import ai.traceable.genai.system.discovery.config.service.v1.Condition;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiInfoExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiNameExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryConfigServiceGrpc;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRuleData;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesResponse;
import ai.traceable.genai.system.discovery.config.service.v1.LeafCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchCondition;
import ai.traceable.genai.system.discovery.config.service.v1.StaticNameAction;
import ai.traceable.genai.system.discovery.info.GenAiSystemDiscoveryConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.kafka.event.listener.KafkaMockConsumerTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiSystemDiscoveryConfigCachingClientImplTest {
  @Mock
  GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceBlockingStub mockStub;

  @Mock
  GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceBlockingStub ongoingStub;

  RequestContext testRequestContext =
      RequestContext.forTenantId("GenAiSystemDiscoveryConfigCacheTest");
  GenAiSystemDiscoveryConfigCachingClientImpl cache;

  @Test
  void test_getGenAiSystemDiscoveryConfigMap() {

    GenAiSystemDiscoveryConfigCachingClientConfig genAiSystemDiscoveryConfigCachingClientConfig =
        GenAiSystemDiscoveryConfigCachingClientConfig.from(
            ConfigFactory.parseMap(
                Map.of(
                    "genai.system.discovery.config.cache.name",
                    "genAiSystemDiscoveryConfigCache",
                    "genai.system.discovery.config.cache.max.size",
                    1000,
                    "genai.system.discovery.config.cache.max.thread.pool.size",
                    2,
                    "genai.system.discovery.config.cache.refresh.duration",
                    10,
                    "genai.system.discovery.config.cache.expiration.duration",
                    20,
                    "genai.system.discovery.config.cache.timeout.duration",
                    5)));

    GenAiSystemDiscoveryRule genAiSystemDiscoveryRule =
        GenAiSystemDiscoveryRule.newBuilder()
            .setRuleId("genAiRuleId")
            .setGenAiSystemDiscoveryRuleData(
                GenAiSystemDiscoveryRuleData.newBuilder()
                    .setName("genai system discovery rule")
                    .setDescription(
                        "genai system discovery rule to discover genai provider and model")
                    .setCondition(
                        Condition.newBuilder()
                            .setLeafCondition(
                                LeafCondition.newBuilder()
                                    .setUrlCondition(
                                        MatchCondition.newBuilder()
                                            .setValue(".*url.*")
                                            .setOperator(OPERATOR_MATCHES_REGEX))))
                    .setGenAiInfoExtractionAction(
                        GenAiInfoExtractionAction.newBuilder()
                            .setProviderAction(
                                GenAiNameExtractionAction.newBuilder()
                                    .setStaticNameAction(
                                        StaticNameAction.newBuilder().setValue("provider")))
                            .setModelAction(
                                GenAiNameExtractionAction.newBuilder()
                                    .setStaticNameAction(
                                        StaticNameAction.newBuilder().setValue("model")))))
            .build();

    when(this.mockStub.withDeadlineAfter(
            genAiSystemDiscoveryConfigCachingClientConfig.getTimeoutDuration().toMillis(),
            MILLISECONDS))
        .thenReturn(this.ongoingStub);
    when(this.ongoingStub.getGenAiSystemDiscoveryRules(
            GetGenAiSystemDiscoveryRulesRequest.newBuilder().build()))
        .thenReturn(
            GetGenAiSystemDiscoveryRulesResponse.newBuilder()
                .addGenAiSystemDiscoveryRules(genAiSystemDiscoveryRule)
                .build());
    Config kafkaConfig =
        ConfigFactory.parseMap(
            Map.of(
                "topic.name",
                "mock-config-change-event",
                "poll.timeout",
                "5ms",
                "consumer.name",
                "mock-config-change-event-consumer"));
    KafkaMockConsumerTestUtil<ConfigChangeEventKey, ConfigChangeEventValue> mockConsumerTestUtil =
        new KafkaMockConsumerTestUtil<>("mock-config-change-event", 1);
    MockConsumer<ConfigChangeEventKey, ConfigChangeEventValue> mockConsumer =
        mockConsumerTestUtil.getMockConsumer();
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>
        kafkaLiveEventListenerBuilder =
            new KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue>()
                .build("mock-config-change-event-consumer", kafkaConfig, mockConsumer);
    this.cache =
        new GenAiSystemDiscoveryConfigCachingClientImpl(
            kafkaLiveEventListenerBuilder,
            this.mockStub,
            genAiSystemDiscoveryConfigCachingClientConfig);
    GenAiSystemDiscoveryConfig genAiSystemDiscoveryConfig =
        this.cache.getGenAiSystemDiscoveryConfig(testRequestContext);
    Map<String, GenAiSystemDiscoveryRule> ruleIdToGenAiSystemDiscoveryRule =
        Map.of("genAiRuleId", genAiSystemDiscoveryRule);
    assertEquals(
        ruleIdToGenAiSystemDiscoveryRule,
        genAiSystemDiscoveryConfig.getGenAiRuleIdToGenAiSystemDiscoveryRuleMap());
  }
}
