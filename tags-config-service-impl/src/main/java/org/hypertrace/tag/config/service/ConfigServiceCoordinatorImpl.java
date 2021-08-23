package org.hypertrace.tag.config.service;

import static org.hypertrace.tag.config.service.TagConfigConstants.TAG_CONFIG;
import static org.hypertrace.tag.config.service.TagConfigConstants.TAG_RESOURCE_NAMESPACE;

import com.google.protobuf.Value;
import com.typesafe.config.Config;
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

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public ConfigServiceCoordinatorImpl(Channel configChannel, Config config) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public Tag upsertTag(RequestContext requestContext, Tag tag) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(TAG_CONFIG)
            .setResourceNamespace(TAG_RESOURCE_NAMESPACE)
            .setConfig(convertToGenericFromTag(tag))
            .setContext(tag.getId())
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return convertToTagFromGeneric(upsertConfigResponse.getConfig());
  }

  public UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.upsertConfig(request));
  }

  @Override
  public Tag getTag(RequestContext requestContext, String tagId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(TAG_CONFIG)
            .setResourceNamespace(TAG_RESOURCE_NAMESPACE)
            .setContexts(0, tagId)
            .build();
    GetConfigResponse getConfigResponse = getConfig(requestContext, getConfigRequest);
    return convertToTagFromGeneric(getConfigResponse.getConfig());
  }

  public GetConfigResponse getConfig(RequestContext context, GetConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.getConfig(request));
  }

  @Override
  public List<Tag> getAllTags(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(TAG_CONFIG)
            .setResourceNamespace(TAG_RESOURCE_NAMESPACE)
            .build();
    GetAllConfigsResponse getAllConfigsResponse =
        getAllConfigs(requestContext, getAllConfigsRequest);
    return getAllConfigsResponse.getContextSpecificConfigsList().stream()
        .map(configResponse -> convertToTagFromGeneric(configResponse.getConfig()))
        .collect(Collectors.toList());
  }

  public GetAllConfigsResponse getAllConfigs(RequestContext context, GetAllConfigsRequest request) {
    return context.call(() -> configServiceBlockingStub.getAllConfigs(request));
  }

  @Override
  public void deleteTag(RequestContext requestContext, String tagId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(TAG_CONFIG)
            .setResourceNamespace(TAG_RESOURCE_NAMESPACE)
            .setContext(tagId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  public DeleteConfigResponse deleteConfig(RequestContext context, DeleteConfigRequest request) {
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
