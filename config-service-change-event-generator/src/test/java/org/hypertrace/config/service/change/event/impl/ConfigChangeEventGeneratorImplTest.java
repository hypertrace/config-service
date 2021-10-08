package org.hypertrace.config.service.change.event.impl;

import static org.mockito.Mockito.verify;

import com.google.protobuf.Value;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigCreateEvent;
import org.hypertrace.config.change.event.v1.ConfigDeleteEvent;
import org.hypertrace.config.change.event.v1.ConfigUpdateEvent;
import org.hypertrace.config.service.change.event.util.KeyUtil;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigChangeEventGeneratorImplTest {

  private static final String TEST_TENANT_ID_1 = "test-tenant";

  private static final String TEST_CONFIG_TYPE = String.class.getName();
  private static final String TEST_CONTEXT = "test-context";
  private static final String TEST_VALUE = "test-value";

  @Mock EventProducer<ConfigChangeEventKey, ConfigChangeEventValue> eventProducer;

  ConfigChangeEventGeneratorImpl changeEventGenerator;
  RequestContext requestContext;

  @BeforeEach
  void setup() {
    changeEventGenerator = new ConfigChangeEventGeneratorImpl(eventProducer);
    requestContext = RequestContext.forTenantId(TEST_TENANT_ID_1);
  }

  @Test
  void sendCreateNotification() {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, TEST_CONTEXT),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(ConfigCreateEvent.newBuilder().setCreatedConfig(config).build())
                .build());
  }

  @Test
  void sendCreateNotificationWithNullContext() {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(requestContext, TEST_CONFIG_TYPE, null, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, null),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(ConfigCreateEvent.newBuilder().setCreatedConfig(config).build())
                .build());
  }

  @Test
  void sendDeleteNotification() {
    Value config = createStringValue();
    changeEventGenerator.sendDeleteNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, TEST_CONTEXT),
            ConfigChangeEventValue.newBuilder()
                .setDeleteEvent(ConfigDeleteEvent.newBuilder().setDeletedConfig(config).build())
                .build());
  }

  @Test
  void sendChangeNotification() {
    Value prevConfig = createStringValue();
    Value latestConfig = createStringValue();

    changeEventGenerator.sendUpdateNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, prevConfig, latestConfig);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, TEST_CONTEXT),
            ConfigChangeEventValue.newBuilder()
                .setUpdateEvent(
                    ConfigUpdateEvent.newBuilder()
                        .setPreviousConfig(prevConfig)
                        .setLatestConfig(latestConfig)
                        .build())
                .build());
  }

  private Value createStringValue() {
    return Value.newBuilder().setStringValue(TEST_VALUE).build();
  }
}
