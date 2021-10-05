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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigChangeEventGeneratorImplTest {

  private static final String TEST_TENANT_ID = "test-tenant";
  private static final String TEST_RESOURCE = "test-resource";
  private static final String TEST_RESOURCE_NAMESPACE = "test-resource-namespace";
  private static final String TEST_CONTEXT = "test-context";
  private static final String TEST_VALUE = "test-value";

  @Mock EventProducer<ConfigChangeEventKey, ConfigChangeEventValue> eventProducer;

  ConfigChangeEventGeneratorImpl changeEventGenerator;

  @BeforeEach
  void setup() {
    changeEventGenerator = new ConfigChangeEventGeneratorImpl(eventProducer);
  }

  @Test
  void sendCreateNotification() {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(
        TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, TEST_CONTEXT),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(ConfigCreateEvent.newBuilder().setCreatedConfig(config).build())
                .build());
  }

  @Test
  void sendCreateNotificationWithNullContext() {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(
        TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, null, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, null),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(ConfigCreateEvent.newBuilder().setCreatedConfig(config).build())
                .build());
  }

  @Test
  void sendDeleteNotification() {
    Value config = createStringValue();
    changeEventGenerator.sendDeleteNotification(
        TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, TEST_CONTEXT),
            ConfigChangeEventValue.newBuilder()
                .setDeleteEvent(ConfigDeleteEvent.newBuilder().setDeletedConfig(config).build())
                .build());
  }

  @Test
  void sendChangeNotification() {
    Value prevConfig = createStringValue();
    Value latestConfig = createStringValue();

    changeEventGenerator.sendUpdateNotification(
        TEST_TENANT_ID,
        TEST_RESOURCE,
        TEST_RESOURCE_NAMESPACE,
        TEST_CONTEXT,
        prevConfig,
        latestConfig);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID, TEST_RESOURCE, TEST_RESOURCE_NAMESPACE, TEST_CONTEXT),
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
