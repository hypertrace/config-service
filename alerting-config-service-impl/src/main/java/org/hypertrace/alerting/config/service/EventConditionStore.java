package org.hypertrace.alerting.config.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.alerting.config.service.v1.EventCondition;
import org.hypertrace.alerting.config.service.v1.EventConditionMutableData;
import org.hypertrace.alerting.config.service.v1.EventConditionMutableData.ConditionCase;
import org.hypertrace.alerting.config.service.v1.NewEventCondition;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EventConditionStore extends IdentifiedObjectStore<EventCondition> {

  private static final String ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME =
      "alertingEventConditionConfig";
  private static final String ALERTING_CONFIG_NAMESPACE = "alerting-v1";

  public EventConditionStore(Channel configChannel) {
    super(
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        ALERTING_CONFIG_NAMESPACE,
        ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME);
  }

  public EventCondition createEventCondition(
      RequestContext requestContext, NewEventCondition newEventCondition) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    if (newEventCondition.getEventConditionData().getConditionCase()
        == ConditionCase.METRIC_ANOMALY_EVENT_CONDITION) {
      builder.setEventConditionData(
          EventConditionMutableData.newBuilder()
              .setMetricAnomalyEventCondition(
                  newEventCondition.getEventConditionData().getMetricAnomalyEventCondition()));
    } else {
      throw new RuntimeException(
          String.format(
              "Condition type is incorrect: %s",
              newEventCondition.getEventConditionData().getConditionCase().name()));
    }

    builder.setId(UUID.randomUUID().toString());

    return upsertObject(requestContext, builder.build());
  }

  @Override
  protected Optional<EventCondition> buildObjectFromValue(Value value) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}, error {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  protected Value buildValueFromObject(EventCondition object) {
    try {
      return ConfigProtoConverter.convertToValue(object);
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}, error {}", object, e);
    }
    return null;
  }

  @Override
  protected String getContextFromObject(EventCondition object) {
    return object.getId();
  }
}
