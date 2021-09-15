package org.hypertrace.alerting.config.service;

import static org.hypertrace.alerting.config.service.AlertingConfigServiceConstants.ALERTING_CONFIG_NAMESPACE;
import static org.hypertrace.alerting.config.service.AlertingConfigServiceConstants.ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME;

import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.alerting.config.service.v1.EventCondition;
import org.hypertrace.alerting.config.service.v1.NewEventCondition;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EventConditionStore {

  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public EventConditionStore(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public EventCondition createEventCondition(
      RequestContext requestContext, NewEventCondition newEventCondition) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    switch (newEventCondition.getConditionCase()) {
      case METRIC_ANOMALY_EVENT_CONDTION:
        builder.setMetricAnomalyEventCondtion(newEventCondition.getMetricAnomalyEventCondtion());
        break;
    }

    builder.setId(UUID.randomUUID().toString());

    return updateEventCondition(requestContext, builder.build());
  }

  public EventCondition updateEventCondition(
      RequestContext requestContext, EventCondition eventCondition) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(eventCondition.getId())
            .setConfig(convertToGeneric(eventCondition))
            .build();

    Value response = upsertConfig(requestContext, upsertConfigRequest);
    return convertFromGeneric(response);
  }

  private Value upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request))
        .getConfig();
  }

  public List<EventCondition> getAllEventConditions(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .build();

    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            requestContext.getRequestHeaders(),
            () -> configServiceBlockingStub.getAllConfigs(getAllConfigsRequest))
        .getContextSpecificConfigsList()
        .stream()
        .map(contextSpecificConfig -> convertFromGeneric(contextSpecificConfig.getConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  public void deleteEventCondition(RequestContext requestContext, String eventConditionId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(eventConditionId)
            .build();
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        requestContext.getRequestHeaders(),
        () -> configServiceBlockingStub.deleteConfig(deleteConfigRequest));
  }

  @SneakyThrows
  private Value convertToGeneric(EventCondition eventCondition) {
    return ConfigProtoConverter.convertToValue(eventCondition);
  }

  @SneakyThrows
  private EventCondition convertFromGeneric(Value value) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return builder.build();
  }
}
