package org.hypertrace.config.service.change.event.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import java.time.Clock;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue.Builder;
import org.hypertrace.config.change.event.v1.ConfigCreateEvent;
import org.hypertrace.config.change.event.v1.ConfigDeleteEvent;
import org.hypertrace.config.change.event.v1.ConfigUpdateEvent;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.change.event.util.KeyUtil;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** Implementation of Config change event generator interface. */
@Slf4j
public class ConfigChangeEventGeneratorImpl implements ConfigChangeEventGenerator {
  private static final String EVENT_STORE = "event.store";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String CONFIG_CHANGE_EVENTS_TOPIC = "config-change-events";
  private static final String CONFIG_CHANGE_EVENTS_PRODUCER_CONFIG =
      "config.change.events.producer";

  private final EventProducer<ConfigChangeEventKey, ConfigChangeEventValue>
      configChangeEventProducer;
  private Clock clock;

  ConfigChangeEventGeneratorImpl(Config appConfig, Clock clock) {
    Config config = appConfig.getConfig(EVENT_STORE);
    this.clock = clock;
    String storeType = config.getString(EVENT_STORE_TYPE_CONFIG);
    EventStore eventStore = EventStoreProvider.getEventStore(storeType, config);
    configChangeEventProducer =
        eventStore.createProducer(
            CONFIG_CHANGE_EVENTS_TOPIC,
            new EventProducerConfig(
                storeType, config.getConfig(CONFIG_CHANGE_EVENTS_PRODUCER_CONFIG)));
  }

  @VisibleForTesting
  ConfigChangeEventGeneratorImpl(
      EventProducer<ConfigChangeEventKey, ConfigChangeEventValue> configChangeEventProducer,
      Clock clock) {
    this.clock = clock;
    this.configChangeEventProducer = configChangeEventProducer;
  }

  @Override
  public void sendCreateNotification(
      RequestContext requestContext, String configType, Value config) {
    produceCreateNotification(requestContext, configType, Optional.empty(), config);
  }

  @Override
  public void sendDeleteNotification(
      RequestContext requestContext, String configType, Value config) {
    produceDeleteNotification(requestContext, configType, Optional.empty(), config);
  }

  @Override
  public void sendUpdateNotification(
      RequestContext requestContext, String configType, Value prevConfig, Value latestConfig) {
    produceUpdateNotification(
        requestContext, configType, Optional.empty(), prevConfig, latestConfig);
  }

  @Override
  public void sendCreateNotification(
      RequestContext requestContext, String configType, String context, Value config) {
    produceCreateNotification(requestContext, configType, Optional.of(context), config);
  }

  @Override
  public void sendDeleteNotification(
      RequestContext requestContext, String configType, String context, Value config) {
    produceDeleteNotification(requestContext, configType, Optional.of(context), config);
  }

  @Override
  public void sendUpdateNotification(
      RequestContext requestContext,
      String configType,
      String context,
      Value prevConfig,
      Value latestConfig) {
    produceUpdateNotification(
        requestContext, configType, Optional.of(context), prevConfig, latestConfig);
  }

  private void produceCreateNotification(
      RequestContext requestContext,
      String configType,
      Optional<String> contextOptional,
      Value config) {
    String tenantId = requestContext.getTenantId().get();
    try {
      Builder builder = ConfigChangeEventValue.newBuilder();
      builder.setCreateEvent(
          ConfigCreateEvent.newBuilder()
              .setCreatedConfigJson(ConfigProtoConverter.convertToJsonString(config))
              .build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      configChangeEventProducer.send(
          KeyUtil.getKey(tenantId, configType, contextOptional), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send create event for config with tenantId {} configType {} context {}",
          tenantId,
          configType,
          contextOptional,
          ex);
    }
  }

  private void produceUpdateNotification(
      RequestContext requestContext,
      String configType,
      Optional<String> contextOptional,
      Value prevConfig,
      Value latestConfig) {
    String tenantId = requestContext.getTenantId().get();
    try {
      Builder builder = ConfigChangeEventValue.newBuilder();
      builder.setUpdateEvent(
          ConfigUpdateEvent.newBuilder()
              .setPreviousConfigJson(ConfigProtoConverter.convertToJsonString(prevConfig))
              .setLatestConfigJson(ConfigProtoConverter.convertToJsonString(latestConfig))
              .build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      configChangeEventProducer.send(
          KeyUtil.getKey(tenantId, configType, contextOptional), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send update event for config with tenantId {} configType {} context {}",
          tenantId,
          configType,
          contextOptional,
          ex);
    }
  }

  private void produceDeleteNotification(
      RequestContext requestContext,
      String configType,
      Optional<String> contextOptional,
      Value config) {
    String tenantId = requestContext.getTenantId().get();
    try {
      Builder builder = ConfigChangeEventValue.newBuilder();
      builder.setDeleteEvent(
          ConfigDeleteEvent.newBuilder()
              .setDeletedConfigJson(ConfigProtoConverter.convertToJsonString(config))
              .build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      configChangeEventProducer.send(
          KeyUtil.getKey(tenantId, configType, contextOptional), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send delete event for config with tenantId {} configType {} context {}",
          tenantId,
          configType,
          contextOptional,
          ex);
    }
  }

  private void populateUserDetails(RequestContext requestContext, Builder builder) {
    requestContext.getUserId().ifPresent(builder::setUserId);
    requestContext.getName().or(requestContext::getEmail).ifPresent(builder::setUserName);
  }
}
