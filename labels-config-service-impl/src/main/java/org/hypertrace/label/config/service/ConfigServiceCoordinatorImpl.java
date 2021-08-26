package org.hypertrace.label.config.service;

import static org.hypertrace.label.config.service.LabelConfigConstants.LABEL_CONFIG_RESOURCE_NAME;
import static org.hypertrace.label.config.service.LabelConfigConstants.LABEL_CONFIG_RESOURCE_NAMESPACE;

import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.Label;

public final class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public ConfigServiceCoordinatorImpl(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public Label upsertLabel(RequestContext requestContext, Label label) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(LABEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_CONFIG_RESOURCE_NAMESPACE)
            .setConfig(convertToGenericFromLabel(label))
            .setContext(label.getId())
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return convertToLabelFromGeneric(upsertConfigResponse.getConfig());
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.upsertConfig(request));
  }

  @Override
  public Label getLabel(RequestContext requestContext, String labelId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(LABEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_CONFIG_RESOURCE_NAMESPACE)
            .addContexts(labelId)
            .build();
    GetConfigResponse getConfigResponse = getConfig(requestContext, getConfigRequest);
    return convertToLabelFromGeneric(getConfigResponse.getConfig());
  }

  private GetConfigResponse getConfig(RequestContext context, GetConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.getConfig(request));
  }

  @Override
  public List<Label> getAllLabels(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(LABEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_CONFIG_RESOURCE_NAMESPACE)
            .build();
    GetAllConfigsResponse getAllConfigsResponse =
        getAllConfigs(requestContext, getAllConfigsRequest);
    return getAllConfigsResponse.getContextSpecificConfigsList().stream()
        .map(configResponse -> convertToLabelFromGeneric(configResponse.getConfig()))
        .collect(Collectors.toList());
  }

  private GetAllConfigsResponse getAllConfigs(
      RequestContext context, GetAllConfigsRequest request) {
    return context.call(() -> configServiceBlockingStub.getAllConfigs(request));
  }

  @Override
  public void deleteLabel(RequestContext requestContext, String labelId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(LABEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_CONFIG_RESOURCE_NAMESPACE)
            .setContext(labelId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  private DeleteConfigResponse deleteConfig(RequestContext context, DeleteConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.deleteConfig(request));
  }

  @SneakyThrows
  private Value convertToGenericFromLabel(Label label) {
    return ConfigProtoConverter.convertToValue(label);
  }

  @SneakyThrows
  private Label convertToLabelFromGeneric(Value value) {
    Label.Builder builder = Label.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return builder.build();
  }
}
