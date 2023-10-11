package org.hypertrace.config.service;

import static org.hypertrace.config.service.ConfigServiceUtils.emptyValue;
import static org.hypertrace.config.service.TestUtils.CONTEXT1;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAME;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAMESPACE;
import static org.hypertrace.config.service.TestUtils.TENANT_ID;
import static org.hypertrace.config.service.TestUtils.getConfig1;
import static org.hypertrace.config.service.TestUtils.getConfig2;
import static org.hypertrace.config.service.TestUtils.getConfigResourceContext;
import static org.hypertrace.config.service.TestUtils.getExpectedMergedConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.config.service.store.ConfigStore;
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
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConfigServiceGrpcImplTest {

  private static Value config1 = getConfig1();
  private static Value config2 = getConfig2();
  private static Value mergedConfig = getExpectedMergedConfig();
  private static ConfigResourceContext configResourceWithoutContext = getConfigResourceContext();
  private static ConfigResourceContext configResourceWithContext =
      getConfigResourceContext(CONTEXT1);

  @Test
  void upsertConfig() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    when(configStore.writeConfig(any(ConfigResourceContext.class), anyString(), any(Value.class)))
        .thenAnswer(
            invocation -> {
              Value config = invocation.getArgument(2, Value.class);
              return UpsertedConfig.newBuilder()
                  .setConfig(config)
                  .setCreationTimestamp(123)
                  .setUpdateTimestamp(456)
                  .build();
            });

    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);

    StreamObserver<UpsertConfigResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnableWithoutContext1 =
        () -> configServiceGrpc.upsertConfig(getUpsertConfigRequest("", config1), responseObserver);
    Runnable runnableWithoutContext2 =
        () -> configServiceGrpc.upsertConfig(getUpsertConfigRequest("", config2), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnableWithoutContext1);
    RequestContext.forTenantId(TENANT_ID).run(runnableWithoutContext2);

    Runnable runnableWithContext =
        () ->
            configServiceGrpc.upsertConfig(
                getUpsertConfigRequest(CONTEXT1, config2), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnableWithContext);

    ArgumentCaptor<UpsertConfigResponse> upsertConfigResponseCaptor =
        ArgumentCaptor.forClass(UpsertConfigResponse.class);
    verify(responseObserver, times(3)).onNext(upsertConfigResponseCaptor.capture());
    verify(responseObserver, times(3)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
    List<UpsertConfigResponse> actualResponseList = upsertConfigResponseCaptor.getAllValues();
    assertEquals(3, actualResponseList.size());
    assertEquals(config1, actualResponseList.get(0).getConfig());
    assertEquals(config2, actualResponseList.get(1).getConfig());
    assertEquals(config2, actualResponseList.get(2).getConfig());
    assertEquals(123L, actualResponseList.get(0).getCreationTimestamp());
    assertEquals(456L, actualResponseList.get(0).getUpdateTimestamp());
  }

  @Test
  void getConfig() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    when(configStore.getConfig(eq(configResourceWithoutContext)))
        .thenReturn(
            ContextSpecificConfig.newBuilder()
                .setConfig(config1)
                .setCreationTimestamp(10L)
                .setUpdateTimestamp(12L)
                .build());
    when(configStore.getConfig(eq(configResourceWithContext)))
        .thenReturn(
            ContextSpecificConfig.newBuilder()
                .setConfig(config2)
                .setContext(CONTEXT1)
                .setCreationTimestamp(15L)
                .setUpdateTimestamp(25L)
                .build());
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<GetConfigResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnableWithoutContext =
        () -> configServiceGrpc.getConfig(getGetConfigRequest(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnableWithoutContext);

    Runnable runnableWithContext =
        () -> configServiceGrpc.getConfig(getGetConfigRequest(CONTEXT1), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnableWithContext);

    ArgumentCaptor<GetConfigResponse> getConfigResponseCaptor =
        ArgumentCaptor.forClass(GetConfigResponse.class);
    verify(responseObserver, times(2)).onNext(getConfigResponseCaptor.capture());
    verify(responseObserver, times(2)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));

    List<GetConfigResponse> actualResponseList = getConfigResponseCaptor.getAllValues();
    assertEquals(2, actualResponseList.size());
    assertEquals(
        GetConfigResponse.newBuilder()
            .setConfig(config1)
            .setCreationTimestamp(10L)
            .setUpdateTimestamp(12L)
            .build(),
        actualResponseList.get(0));
    assertEquals(
        GetConfigResponse.newBuilder()
            .setConfig(mergedConfig)
            .setCreationTimestamp(15L)
            .setUpdateTimestamp(25L)
            .build(),
        actualResponseList.get(1));
  }

  @Test
  void getAllConfigs() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    List<ContextSpecificConfig> contextSpecificConfigList = new ArrayList<>();
    contextSpecificConfigList.add(ContextSpecificConfig.newBuilder().setConfig(config1).build());
    contextSpecificConfigList.add(
        ContextSpecificConfig.newBuilder().setContext(CONTEXT1).setConfig(config2).build());
    when(configStore.getAllConfigs(
            new ConfigResource(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_ID)))
        .thenReturn(contextSpecificConfigList);
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<GetAllConfigsResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnable =
        () -> configServiceGrpc.getAllConfigs(getGetAllConfigsRequest(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    ArgumentCaptor<GetAllConfigsResponse> getAllConfigsResponseCaptor =
        ArgumentCaptor.forClass(GetAllConfigsResponse.class);
    verify(responseObserver, times(1)).onNext(getAllConfigsResponseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));

    GetAllConfigsResponse actualResponse = getAllConfigsResponseCaptor.getValue();
    assertEquals(contextSpecificConfigList, actualResponse.getContextSpecificConfigsList());
  }

  @Test
  void deleteConfig() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    ContextSpecificConfig deletedConfig =
        ContextSpecificConfig.newBuilder().setConfig(config2).setContext(CONTEXT1).build();
    when(configStore.getConfig(eq(configResourceWithContext))).thenReturn(deletedConfig);
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<DeleteConfigResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnable =
        () -> configServiceGrpc.deleteConfig(getDeleteConfigRequest(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    verify(configStore, times(1)).deleteConfigs(eq(Set.of(configResourceWithContext)));
    verify(responseObserver, times(1))
        .onNext(eq(DeleteConfigResponse.newBuilder().setDeletedConfig(deletedConfig).build()));
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
  }

  @Test
  void deleteConfigs() throws IOException {
    String context1 = "context1";
    String context2 = "context2";
    ConfigStore configStore = mock(ConfigStore.class);
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<DeleteConfigsResponse> responseObserver = mock(StreamObserver.class);

    DeleteConfigsRequest deleteConfigsRequest =
        DeleteConfigsRequest.newBuilder()
            .addConfigs(getConfigToDelete(context1))
            .addConfigs(getConfigToDelete(context2))
            .build();

    Map<ConfigResourceContext, Value> writeAllConfigsRequest =
        Map.of(
            getConfigResourceContext(context1),
            emptyValue(),
            getConfigResourceContext(context2),
            emptyValue());

    when(configStore.getAllContextConfigs(writeAllConfigsRequest.keySet()))
        .thenReturn(
            Map.of(
                getConfigResourceContext(context1),
                    buildContextSpecificConfig(context1, config1, 10L, 20L),
                getConfigResourceContext(context2),
                    buildContextSpecificConfig(context2, config2, 10L, 20L)));
    Runnable runnable =
        () -> configServiceGrpc.deleteConfigs(deleteConfigsRequest, responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);
    verify(configStore, times(1))
        .deleteConfigs(
            eq(Set.of(getConfigResourceContext(context1), getConfigResourceContext(context2))));
    verify(responseObserver, times(1))
        .onNext(
            DeleteConfigsResponse.newBuilder()
                .addAllDeletedConfigs(
                    List.of(
                        buildContextSpecificConfig(context1, config1, 10L, 20L),
                        buildContextSpecificConfig(context2, config2, 10L, 20L)))
                .build());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));

    runnable =
        () ->
            configServiceGrpc.deleteConfigs(
                DeleteConfigsRequest.getDefaultInstance(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);
    verify(responseObserver, times(1)).onError(any(Throwable.class));
  }

  @Test
  void deleteDefaultContextConfig() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    ContextSpecificConfig deletedConfig =
        ContextSpecificConfig.newBuilder().setConfig(config2).build();
    when(configStore.getConfig(eq(configResourceWithoutContext))).thenReturn(deletedConfig);
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<DeleteConfigResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            configServiceGrpc.deleteConfig(
                getDefaultContextDeleteConfigRequest(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    verify(configStore, times(1)).deleteConfigs(eq(Set.of(configResourceWithoutContext)));
    verify(responseObserver, times(1))
        .onNext(eq(DeleteConfigResponse.newBuilder().setDeletedConfig(deletedConfig).build()));
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
  }

  @Test
  void deletingNonExistingConfigShouldThrowError() throws IOException {
    ConfigStore configStore = mock(ConfigStore.class);
    ContextSpecificConfig emptyConfig = ConfigServiceUtils.emptyConfig(CONTEXT1);
    when(configStore.getConfig(eq(configResourceWithContext))).thenReturn(emptyConfig);
    ConfigServiceGrpcImpl configServiceGrpc = new ConfigServiceGrpcImpl(configStore);
    StreamObserver<DeleteConfigResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnable =
        () -> configServiceGrpc.deleteConfig(getDeleteConfigRequest(), responseObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver, times(1)).onError(throwableArgumentCaptor.capture());
    Throwable throwable = throwableArgumentCaptor.getValue();
    assertTrue(throwable instanceof StatusException);
    assertEquals(Status.NOT_FOUND, ((StatusException) throwable).getStatus());

    verify(configStore, never())
        .writeConfig(any(ConfigResourceContext.class), anyString(), any(Value.class));
    verify(responseObserver, never()).onNext(any(DeleteConfigResponse.class));
    verify(responseObserver, never()).onCompleted();
  }

  private UpsertConfigRequest getUpsertConfigRequest(String context, Value config) {
    return UpsertConfigRequest.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .setContext(context)
        .setConfig(config)
        .build();
  }

  private GetConfigRequest getGetConfigRequest(String... contexts) {
    return GetConfigRequest.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .addAllContexts(Arrays.asList(contexts))
        .build();
  }

  private GetAllConfigsRequest getGetAllConfigsRequest() {
    return GetAllConfigsRequest.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .build();
  }

  private DeleteConfigRequest getDeleteConfigRequest() {
    return DeleteConfigRequest.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .setContext(CONTEXT1)
        .build();
  }

  private DeleteConfigRequest getDefaultContextDeleteConfigRequest() {
    return DeleteConfigRequest.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .build();
  }

  private ConfigToDelete getConfigToDelete(String context) {
    return ConfigToDelete.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .setContext(context)
        .build();
  }

  private UpsertedConfig buildUpsertedConfig(
      String context,
      Value config,
      Value prevConfig,
      long creationTimestamp,
      long updateTimestamp) {
    return UpsertedConfig.newBuilder()
        .setContext(context)
        .setConfig(config)
        .setPrevConfig(prevConfig)
        .setCreationTimestamp(creationTimestamp)
        .setUpdateTimestamp(updateTimestamp)
        .build();
  }

  private ContextSpecificConfig buildContextSpecificConfig(
      String context, Value config, long creationTimestamp, long updateTimestamp) {
    return ContextSpecificConfig.newBuilder()
        .setContext(context)
        .setConfig(config)
        .setCreationTimestamp(creationTimestamp)
        .setUpdateTimestamp(updateTimestamp)
        .build();
  }
}
