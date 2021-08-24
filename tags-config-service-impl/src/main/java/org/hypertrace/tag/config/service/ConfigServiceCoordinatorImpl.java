package org.hypertrace.tag.config.service;

import static org.hypertrace.tag.config.service.TagConfigConstants.TAG_CONFIG_RESOURCE_NAME;
import static org.hypertrace.tag.config.service.TagConfigConstants.TAG_CONFIG_RESOURCE_NAMESPACE;

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
import org.hypertrace.tag.config.service.v1.Tag;

public final class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public ConfigServiceCoordinatorImpl(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public Tag upsertTag(RequestContext requestContext, Tag tag) {
    try {
      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(TAG_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(TAG_CONFIG_RESOURCE_NAMESPACE)
              .setConfig(convertToGenericFromTag(tag))
              .setContext(tag.getId())
              .build();
      UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
      return convertToTagFromGeneric(upsertConfigResponse.getConfig());
    } catch (Exception e) {
      throw e;
    }
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.upsertConfig(request));
  }

  @Override
  public Tag getTag(RequestContext requestContext, String tagId) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .setResourceName(TAG_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(TAG_CONFIG_RESOURCE_NAMESPACE)
              .addContexts(tagId)
              .build();
      GetConfigResponse getConfigResponse = getConfig(requestContext, getConfigRequest);
      return convertToTagFromGeneric(getConfigResponse.getConfig());
    } catch (Exception e) {
      throw e;
    }
  }

  private GetConfigResponse getConfig(RequestContext context, GetConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.getConfig(request));
  }

  @Override
  public List<Tag> getAllTags(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(TAG_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(TAG_CONFIG_RESOURCE_NAMESPACE)
            .build();
    GetAllConfigsResponse getAllConfigsResponse =
        getAllConfigs(requestContext, getAllConfigsRequest);
    return getAllConfigsResponse.getContextSpecificConfigsList().stream()
        .map(configResponse -> convertToTagFromGeneric(configResponse.getConfig()))
        .collect(Collectors.toList());
  }

  private GetAllConfigsResponse getAllConfigs(
      RequestContext context, GetAllConfigsRequest request) {
    return context.call(() -> configServiceBlockingStub.getAllConfigs(request));
  }

  @Override
  public void deleteTag(RequestContext requestContext, String tagId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(TAG_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(TAG_CONFIG_RESOURCE_NAMESPACE)
            .setContext(tagId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  private DeleteConfigResponse deleteConfig(RequestContext context, DeleteConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.deleteConfig(request));
  }

  @SneakyThrows
  private Value convertToGenericFromTag(Tag tag) {
    return ConfigProtoConverter.convertToValue(tag);
  }

  @SneakyThrows
  private Tag convertToTagFromGeneric(Value value) {
    Tag.Builder builder = Tag.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return builder.build();
  }
}
