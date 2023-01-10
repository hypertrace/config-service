package org.hypertrace.config.service;

import static org.hypertrace.config.service.ConfigServiceUtils.emptyValue;
import static org.hypertrace.config.service.ConfigServiceUtils.filterNull;
import static org.hypertrace.config.service.ConfigServiceUtils.merge;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.DeleteConfigsRequest;
import org.hypertrace.config.service.v1.DeleteConfigsRequest.ConfigToDelete;
import org.hypertrace.config.service.v1.DeleteConfigsResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest.ConfigToUpsert;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** Implementation for the gRPC service. */
@Slf4j
public class ConfigServiceGrpcImpl extends ConfigServiceGrpc.ConfigServiceImplBase {

  private final ConfigStore configStore;

  public ConfigServiceGrpcImpl(ConfigStore configStore) {
    this.configStore = configStore;
  }

  @Override
  public void upsertConfig(
      UpsertConfigRequest request, StreamObserver<UpsertConfigResponse> responseObserver) {
    try {
      ConfigResourceContext configResourceContext = getConfigResourceContext(request);
      UpsertedConfig upsertedConfig =
          configStore.writeConfig(configResourceContext, getUserId(), request.getConfig());
      UpsertConfigResponse.Builder builder = UpsertConfigResponse.newBuilder();
      builder.setConfig(request.getConfig());
      builder.setCreationTimestamp(upsertedConfig.getCreationTimestamp());
      builder.setUpdateTimestamp(upsertedConfig.getUpdateTimestamp());
      if (upsertedConfig.hasPrevConfig()) {
        builder.setPrevConfig(upsertedConfig.getPrevConfig());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Upsert failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getConfig(
      GetConfigRequest request, StreamObserver<GetConfigResponse> responseObserver) {
    try {
      ContextSpecificConfig config = configStore.getConfig(getConfigResourceContext(request));

      // get the configs for the contexts mentioned in the request and merge them in the specified
      // order
      for (String context : request.getContextsList()) {
        ContextSpecificConfig contextSpecificConfig =
            configStore.getConfig(getConfigResourceContext(request, context));
        config = merge(config, contextSpecificConfig);
      }

      filterNull(config)
          .map(
              nonNullConfig ->
                  GetConfigResponse.newBuilder()
                      .setConfig(nonNullConfig.getConfig())
                      .setCreationTimestamp(nonNullConfig.getCreationTimestamp())
                      .setUpdateTimestamp(nonNullConfig.getUpdateTimestamp())
                      .build())
          .ifPresentOrElse(
              response -> {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
              },
              () -> responseObserver.onError(Status.NOT_FOUND.asException()));

    } catch (Exception e) {
      log.error("Get config failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllConfigs(
      GetAllConfigsRequest request, StreamObserver<GetAllConfigsResponse> responseObserver) {
    try {
      List<ContextSpecificConfig> contextSpecificConfigList =
          configStore.getAllConfigs(
              new ConfigResource(
                  request.getResourceName(), request.getResourceNamespace(), getTenantId()));
      responseObserver.onNext(
          GetAllConfigsResponse.newBuilder()
              .addAllContextSpecificConfigs(contextSpecificConfigList)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get all configs failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteConfig(
      DeleteConfigRequest request, StreamObserver<DeleteConfigResponse> responseObserver) {
    try {
      ConfigResourceContext configResourceContext = getConfigResourceContext(request);
      ContextSpecificConfig configToDelete = configStore.getConfig(configResourceContext);

      // if configToDelete is null/empty (i.e. config value doesn't exist or is already deleted),
      // then throw NOT_FOUND exception
      if (ConfigServiceUtils.isNull(configToDelete.getConfig())) {
        responseObserver.onError(Status.NOT_FOUND.asException());
        return;
      }

      // write an empty config for the specified config resource. This maintains the versioning.
      configStore.writeConfig(configResourceContext, getUserId(), emptyValue());
      responseObserver.onNext(
          DeleteConfigResponse.newBuilder().setDeletedConfig(configToDelete).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete config failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteConfigs(
      DeleteConfigsRequest request, StreamObserver<DeleteConfigsResponse> responseObserver) {
    try {
      if (request.getConfigsCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("List of configs to delete provided is empty")
                .asException());
        return;
      }
      Map<ConfigResourceContext, Value> valuesByContext =
          request.getConfigsList().stream()
              .map(
                  requestedDelete ->
                      Map.entry(this.getConfigResourceContext(requestedDelete), emptyValue()))
              .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

      List<UpsertedConfig> deletedConfigs =
          configStore.writeAllConfigs(valuesByContext, getUserId());

      responseObserver.onNext(
          DeleteConfigsResponse.newBuilder()
              .addAllDeletedConfigs(
                  deletedConfigs.stream()
                      .map(this::buildDeletedContextSpecificConfig)
                      .collect(Collectors.toUnmodifiableList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete configs failed for request: {}", request, e);
    }
  }

  private ContextSpecificConfig buildDeletedContextSpecificConfig(UpsertedConfig deletedConfig) {
    return ContextSpecificConfig.newBuilder()
        .setContext(deletedConfig.getContext())
        .setCreationTimestamp(deletedConfig.getCreationTimestamp())
        .setUpdateTimestamp(deletedConfig.getUpdateTimestamp())
        .setConfig(deletedConfig.getPrevConfig())
        .build();
  }

  @Override
  public void upsertAllConfigs(
      UpsertAllConfigsRequest request, StreamObserver<UpsertAllConfigsResponse> responseObserver) {
    try {
      if (request.getConfigsCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("List of configs to upsert provided is empty")
                .asException());
        return;
      }
      Map<ConfigResourceContext, Value> valuesByContext =
          request.getConfigsList().stream()
              .map(
                  requestedUpsert ->
                      Map.entry(
                          this.getConfigResourceContext(requestedUpsert),
                          requestedUpsert.getConfig()))
              .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
      List<UpsertedConfig> upsertedConfigs =
          configStore.writeAllConfigs(valuesByContext, getUserId());
      responseObserver.onNext(
          UpsertAllConfigsResponse.newBuilder().addAllUpsertedConfigs(upsertedConfigs).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Upsert all failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private ConfigResourceContext getConfigResourceContext(UpsertConfigRequest upsertConfigRequest) {
    return new ConfigResourceContext(
        new ConfigResource(
            upsertConfigRequest.getResourceName(),
            upsertConfigRequest.getResourceNamespace(),
            getTenantId()),
        upsertConfigRequest.getContext());
  }

  private ConfigResourceContext getConfigResourceContext(DeleteConfigRequest upsertConfigRequest) {
    return new ConfigResourceContext(
        new ConfigResource(
            upsertConfigRequest.getResourceName(),
            upsertConfigRequest.getResourceNamespace(),
            getTenantId()),
        upsertConfigRequest.getContext());
  }

  private ConfigResourceContext getConfigResourceContext(GetConfigRequest getConfigRequest) {
    return new ConfigResourceContext(
        new ConfigResource(
            getConfigRequest.getResourceName(),
            getConfigRequest.getResourceNamespace(),
            getTenantId()));
  }

  private ConfigResourceContext getConfigResourceContext(
      GetConfigRequest getConfigRequest, String context) {
    return new ConfigResourceContext(
        new ConfigResource(
            getConfigRequest.getResourceName(),
            getConfigRequest.getResourceNamespace(),
            getTenantId()),
        context);
  }

  private ConfigResourceContext getConfigResourceContext(ConfigToUpsert configToUpsert) {
    return new ConfigResourceContext(
        new ConfigResource(
            configToUpsert.getResourceName(), configToUpsert.getResourceNamespace(), getTenantId()),
        configToUpsert.getContext());
  }

  private ConfigResourceContext getConfigResourceContext(ConfigToDelete configToDelete) {
    return new ConfigResourceContext(
        new ConfigResource(
            configToDelete.getResourceName(), configToDelete.getResourceNamespace(), getTenantId()),
        configToDelete.getContext());
  }

  private String getTenantId() {
    return RequestContext.CURRENT
        .get()
        .getTenantId()
        .orElseThrow(
            Status.INVALID_ARGUMENT.withDescription("Tenant ID is missing in the request")
                ::asRuntimeException);
  }

  // TODO : get the userId from the context
  private String getUserId() {
    return "";
  }
}
