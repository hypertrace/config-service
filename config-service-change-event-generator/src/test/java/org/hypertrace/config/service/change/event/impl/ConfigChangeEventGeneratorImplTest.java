package org.hypertrace.config.service.change.event.impl;

import static org.mockito.Mockito.verify;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigCreateEvent;
import org.hypertrace.config.change.event.v1.ConfigDeleteEvent;
import org.hypertrace.config.change.event.v1.ConfigUpdateEvent;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
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
  private static final String TEST_NEW_VALUE = "test-new-value";

  @Mock EventProducer<ConfigChangeEventKey, ConfigChangeEventValue> eventProducer;

  ConfigChangeEventGeneratorImpl changeEventGenerator;
  RequestContext requestContext;

  @BeforeEach
  void setup() {
    changeEventGenerator = new ConfigChangeEventGeneratorImpl(eventProducer);
    requestContext = RequestContext.forTenantId(TEST_TENANT_ID_1);
  }

  @Test
  void sendCreateNotification() throws InvalidProtocolBufferException {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.of(TEST_CONTEXT)),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(
                    ConfigCreateEvent.newBuilder()
                        .setCreatedConfigJson(ConfigProtoConverter.convertToJsonString(config))
                        .build())
                .build());
  }

  @Test
  void sendCreateNotificationWithNoContext() throws InvalidProtocolBufferException {
    Value config = createStringValue();
    changeEventGenerator.sendCreateNotification(requestContext, TEST_CONFIG_TYPE, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.empty()),
            ConfigChangeEventValue.newBuilder()
                .setCreateEvent(
                    ConfigCreateEvent.newBuilder()
                        .setCreatedConfigJson(ConfigProtoConverter.convertToJsonString(config))
                        .build())
                .build());
  }

  @Test
  void sendDeleteNotification() throws InvalidProtocolBufferException {
    Value config = createStringValue();
    changeEventGenerator.sendDeleteNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.of(TEST_CONTEXT)),
            ConfigChangeEventValue.newBuilder()
                .setDeleteEvent(
                    ConfigDeleteEvent.newBuilder()
                        .setDeletedConfigJson(ConfigProtoConverter.convertToJsonString(config))
                        .build())
                .build());
  }

  @Test
  void sendDeleteNotificationWithNoContext() throws InvalidProtocolBufferException {
    Value config = createStringValue();
    changeEventGenerator.sendDeleteNotification(requestContext, TEST_CONFIG_TYPE, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.empty()),
            ConfigChangeEventValue.newBuilder()
                .setDeleteEvent(
                    ConfigDeleteEvent.newBuilder()
                        .setDeletedConfigJson(ConfigProtoConverter.convertToJsonString(config))
                        .build())
                .build());
  }

  @Test
  void sendChangeNotification() throws InvalidProtocolBufferException {
    Value prevConfig = createStringValue();
    Value latestConfig = createStringValue(TEST_NEW_VALUE);

    changeEventGenerator.sendUpdateNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, prevConfig, latestConfig);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.of(TEST_CONTEXT)),
            ConfigChangeEventValue.newBuilder()
                .setUpdateEvent(
                    ConfigUpdateEvent.newBuilder()
                        .setPreviousConfigJson(ConfigProtoConverter.convertToJsonString(prevConfig))
                        .setLatestConfigJson(ConfigProtoConverter.convertToJsonString(latestConfig))
                        .build())
                .build());
  }

  @Test
  void sendChangeNotificationWithNoContext() throws InvalidProtocolBufferException {
    Value prevConfig = createStringValue();
    Value latestConfig = createStringValue(TEST_NEW_VALUE);

    changeEventGenerator.sendUpdateNotification(
        requestContext, TEST_CONFIG_TYPE, prevConfig, latestConfig);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.empty()),
            ConfigChangeEventValue.newBuilder()
                .setUpdateEvent(
                    ConfigUpdateEvent.newBuilder()
                        .setPreviousConfigJson(ConfigProtoConverter.convertToJsonString(prevConfig))
                        .setLatestConfigJson(ConfigProtoConverter.convertToJsonString(latestConfig))
                        .build())
                .build());
  }

  private Value createStringValue() {
    return createStringValue(TEST_VALUE);
  }

  private Value createStringValue(String value) {
    return Value.newBuilder().setStringValue(value).build();
  }
}
