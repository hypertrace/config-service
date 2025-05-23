package org.hypertrace.config.service;

import static org.hypertrace.config.service.ConfigServiceUtils.emptyConfig;
import static org.hypertrace.config.service.ConfigServiceUtils.filterNull;
import static org.hypertrace.config.service.ConfigServiceUtils.merge;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

  private static String DEFAULT_USER_ID = "Unknown";
  private static String DEFAULT_USER_EMAIL = "Unknown";
  private final ConfigStore configStore;
  private static final Executor configExecutor =
      Executors.newFixedThreadPool(
          4,
          new ThreadFactoryBuilder().setNameFormat("config-executor-%d").setDaemon(true).build());

  public ConfigServiceGrpcImpl(ConfigStore configStore) {
    this.configStore = configStore;
  }

  @Override
  public void upsertConfig(
      UpsertConfigRequest request, StreamObserver<UpsertConfigResponse> responseObserver) {
    try {
      ConfigResourceContext configResourceContext = getConfigResourceContext(request);
      UpsertedConfig upsertedConfig =
          configStore.writeConfig(configResourceContext, getUserId(), request, getUserEmail());
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
      ConfigResourceContext configResourceContext = getConfigResourceContext(request);
      ContextSpecificConfig config =
          configStore
              .getConfig(configResourceContext)
              .orElse(emptyConfig(configResourceContext.getContext()));
      // get the configs for the contexts mentioned in the request and merge them in the specified
      // order
      for (String context : request.getContextsList()) {
        Optional<ContextSpecificConfig> maybeContextConfig =
            configStore.getConfig(getConfigResourceContext(request, context));
        ContextSpecificConfig lastConfig = config;
        config =
            maybeContextConfig
                .map(contextConfig -> merge(lastConfig, contextConfig))
                .orElse(config);
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
      ConfigResource configResource =
          new ConfigResource(
              request.getResourceName(), request.getResourceNamespace(), getTenantId());

      CompletableFuture<Long> totalCountFuture = null;

      // Start count query first (in parallel)
      if (request.getIncludeTotal()) {
        totalCountFuture =
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    return configStore.getMatchingConfigsCount(configResource, request.getFilter());
                  } catch (Exception e) {
                    throw Status.INTERNAL
                        .withCause(e)
                        .withDescription("Failed to fetch total count")
                        .asRuntimeException();
                  }
                },
                configExecutor);
      }

      // Then run getAllConfigs on the current thread
      List<ContextSpecificConfig> configList =
          configStore.getAllConfigs(
              configResource,
              request.getFilter(),
              request.getPagination(),
              request.getSortByList());

      // Build the response
      GetAllConfigsResponse.Builder responseBuilder =
          GetAllConfigsResponse.newBuilder().addAllContextSpecificConfigs(configList);

      if (totalCountFuture != null) {
        long totalCount = totalCountFuture.join(); // Wait if not finished
        responseBuilder.setTotalCount(totalCount);
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get all configs failed for request: {}", request, e);
      responseObserver.onError(Status.fromThrowable(e).asRuntimeException());
    }
  }

  @Override
  public void deleteConfig(
      DeleteConfigRequest request, StreamObserver<DeleteConfigResponse> responseObserver) {

    try {
      ConfigResourceContext configResourceContext = getConfigResourceContext(request);
      DeleteConfigResponse deleteResponse =
          configStore
              .getConfig(configResourceContext)
              .map(
                  configToDelete ->
                      DeleteConfigResponse.newBuilder().setDeletedConfig(configToDelete).build())
              .orElseThrow(Status.NOT_FOUND::asException);

      // delete the config for the specified config resource.
      configStore.deleteConfigs(Set.of(configResourceContext));
      responseObserver.onNext(deleteResponse);
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

      List<ConfigResourceContext> configResourceContexts =
          request.getConfigsList().stream()
              .map(this::getConfigResourceContext)
              .collect(Collectors.toUnmodifiableList());
      Map<ConfigResourceContext, ContextSpecificConfig> configs =
          configStore.getContextConfigs(configResourceContexts);
      // delete the configs for the specified config resources.
      configStore.deleteConfigs(configs.keySet());
      responseObserver.onNext(
          DeleteConfigsResponse.newBuilder().addAllDeletedConfigs(configs.values()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete configs failed for request: {}", request, e);
      responseObserver.onError(e);
    }
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
          configStore.writeAllConfigs(valuesByContext, getUserId(), getUserEmail());
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

  private String getUserEmail() {
    return RequestContext.CURRENT.get().getEmail().orElse(DEFAULT_USER_EMAIL);
  }

  private String getUserId() {
    return RequestContext.CURRENT.get().getUserId().orElse(DEFAULT_USER_ID);
  }
}
