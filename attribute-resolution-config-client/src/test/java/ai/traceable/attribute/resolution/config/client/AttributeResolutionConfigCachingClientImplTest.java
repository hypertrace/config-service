package ai.traceable.attribute.resolution.config.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.attribute.resolution.client.AttributeResolutionConfigCachingClientImpl;
import ai.traceable.attribute.resolution.client.AttributeResolutionConfigClient;
import ai.traceable.attribute.resolution.config.AttributeResolutionConfigCachingClientConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigData;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigServiceGrpc;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsResponse;
import ai.traceable.attribute.resolution.info.AttributeResolutionConfigInfo;
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
class AttributeResolutionConfigCachingClientImplTest {
  @Mock AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceBlockingStub mockStub;

  @Mock
  AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceBlockingStub ongoingStub;

  private final RequestContext testRequestContext = RequestContext.forTenantId("demo-tenant");

  @Test
  void test_getAttributeResolutionConfig_returnsExpectedMap() {
    AttributeResolutionConfigCachingClientConfig clientConfig =
        AttributeResolutionConfigCachingClientConfig.from(
            ConfigFactory.parseMap(
                Map.of(
                    "attribute.resolution.config.cache.name",
                    "attributeResolutionConfigCache",
                    "attribute.resolution.config.cache.max.size",
                    1000,
                    "attribute.resolution.config.cache.max.thread.pool.size",
                    1,
                    "attribute.resolution.config.cache.refresh.duration",
                    10,
                    "attribute.resolution.config.cache.expiration.duration",
                    20,
                    "attribute.resolution.config.cache.timeout.duration",
                    5)));

    AttributeResolutionConfig sampleConfig =
        AttributeResolutionConfig.newBuilder()
            .setId("config-1")
            .setData(
                AttributeResolutionConfigData.newBuilder()
                    .setName("sample")
                    .setAttributeKey("version")
                    .setEnabled(true)
                    .build())
            .build();

    when(this.mockStub.withDeadlineAfter(
            clientConfig.getTimeoutDuration().toMillis(), MILLISECONDS))
        .thenReturn(this.ongoingStub);
    when(this.ongoingStub.getAttributeResolutionConfigs(
            GetAttributeResolutionConfigsRequest.newBuilder().build()))
        .thenReturn(
            GetAttributeResolutionConfigsResponse.newBuilder().addConfigs(sampleConfig).build());

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
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener =
        new KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue>()
            .build("mock-consumer", kafkaConfig, mockConsumer);

    AttributeResolutionConfigClient cache =
        new AttributeResolutionConfigCachingClientImpl(
            kafkaLiveEventListener, this.mockStub, clientConfig);

    AttributeResolutionConfigInfo info = cache.getAttributeResolutionConfig(testRequestContext);

    assertEquals(Map.of("config-1", sampleConfig), info.getIdToAttributeResolutionConfigMap());
  }
}
