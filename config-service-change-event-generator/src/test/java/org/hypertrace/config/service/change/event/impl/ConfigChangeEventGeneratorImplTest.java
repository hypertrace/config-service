package org.hypertrace.config.service.change.event.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.time.Clock;
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
  private static final String TEST_USER_ID = "test-user-id";
  private static final String TEST_USER_NAME = "test-user-name";
  private static final String TEST_USER_EMAIL = "test-user-email";
  private static final long CURRENT_TIME_MILLIS = 1000;

  @Mock EventProducer<ConfigChangeEventKey, ConfigChangeEventValue> eventProducer;

  ConfigChangeEventGeneratorImpl changeEventGenerator;
  RequestContext requestContext;
  private Clock mockClock;

  @BeforeEach
  void setup() {
    mockClock = mock(Clock.class);
    when(mockClock.millis()).thenReturn(CURRENT_TIME_MILLIS);
    changeEventGenerator = new ConfigChangeEventGeneratorImpl(eventProducer, mockClock);
    requestContext = mock(RequestContext.class);
    when(requestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID_1));
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .setCreateEvent(
                    ConfigCreateEvent.newBuilder()
                        .setCreatedConfigJson(ConfigProtoConverter.convertToJsonString(config))
                        .build())
                .build());
  }

  @Test
  void sendCreateNotificationWithUserDetailsInRequestContext()
      throws InvalidProtocolBufferException {
    Value config = createStringValue();
    when(requestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(requestContext.getName()).thenReturn(Optional.of(TEST_USER_NAME));
    when(requestContext.getEmail()).thenReturn(Optional.of(TEST_USER_EMAIL));
    changeEventGenerator.sendCreateNotification(
        requestContext, TEST_CONFIG_TYPE, TEST_CONTEXT, config);
    verify(eventProducer)
        .send(
            KeyUtil.getKey(TEST_TENANT_ID_1, TEST_CONFIG_TYPE, Optional.of(TEST_CONTEXT)),
            ConfigChangeEventValue.newBuilder()
                .setUserId(TEST_USER_ID)
                .setUserEmail(TEST_USER_EMAIL)
                .setUserName(TEST_USER_NAME)
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
