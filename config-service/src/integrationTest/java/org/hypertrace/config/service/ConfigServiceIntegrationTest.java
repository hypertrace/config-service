package org.hypertrace.config.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.v1.*;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

public class ConfigServiceIntegrationTest extends ConfigServiceIntegrationTestBase {

  @Test
  public void testUpsertConfig() {
    // upsert first version of config for tenant-1
    UpsertConfigResponse upsertConfigResponse =
        upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.empty(), config1);
    assertEquals(config1, upsertConfigResponse.getConfig());

    // upsert second version of config for tenant-1
    upsertConfigResponse =
        upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.empty(), config2);
    assertEquals(config2, upsertConfigResponse.getConfig());

    // test across tenants - upsert first version of config for tenant-2
    upsertConfigResponse =
        upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_2, Optional.empty(), config1);
    assertEquals(config1, upsertConfigResponse.getConfig());
  }

  @Test
  public void testUpsertAllConfigs() {
    UpsertAllConfigsRequest upsertAllConfigsRequest =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_1, config1),
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_2, config2)))
            .build();
    UpsertAllConfigsResponse upsertAllConfigsResponse =
        upsertConfigs(upsertAllConfigsRequest, TENANT_1);
    assertEquals(2, upsertAllConfigsResponse.getUpsertedConfigsCount());
    UpsertAllConfigsResponse.UpsertedConfig upsertedConfig1 =
        upsertAllConfigsResponse.getUpsertedConfigs(0);
    assertEquals(CONTEXT_1, upsertedConfig1.getContext());
    assertEquals(config1, upsertedConfig1.getConfig());
    assertFalse(upsertedConfig1.hasPrevConfig());
    UpsertAllConfigsResponse.UpsertedConfig upsertedConfig2 =
        upsertAllConfigsResponse.getUpsertedConfigs(1);
    assertEquals(CONTEXT_2, upsertedConfig2.getContext());
    assertEquals(config2, upsertedConfig2.getConfig());
    assertFalse(upsertedConfig2.hasPrevConfig());

    // Swap configs between contexts as an update
    upsertAllConfigsRequest =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_1, config2),
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_2, config1)))
            .build();
    upsertAllConfigsResponse = upsertConfigs(upsertAllConfigsRequest, TENANT_1);
    assertEquals(2, upsertAllConfigsResponse.getUpsertedConfigsCount());
    upsertedConfig1 = upsertAllConfigsResponse.getUpsertedConfigs(0);
    assertEquals(CONTEXT_1, upsertedConfig1.getContext());
    assertEquals(config2, upsertedConfig1.getConfig());
    assertEquals(config1, upsertedConfig1.getPrevConfig());
    upsertedConfig2 = upsertAllConfigsResponse.getUpsertedConfigs(1);
    assertEquals(CONTEXT_2, upsertedConfig2.getContext());
    assertEquals(config1, upsertedConfig2.getConfig());
    assertEquals(config2, upsertedConfig2.getPrevConfig());
  }

  @Test
  public void testGetConfig() {
    // upsert config with default or no context
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.empty(), config1);

    // upsert config with context
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.of(CONTEXT_1), config2);

    // get default config (without context)
    GetConfigResponse getDefaultConfigResponse =
        getConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1);
    assertEquals(config1, getDefaultConfigResponse.getConfig());

    // get config with context as CONTEXT_1 - should merge the first two configs(which is config3)
    GetConfigResponse getContext1ConfigResponse =
        getConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_1);
    assertEquals(config3, getContext1ConfigResponse.getConfig());

    // get config with context as CONTEXT_2 - should return the config with default context
    GetConfigResponse getContext2ConfigResponse =
        getConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_2);
    assertEquals(config1, getContext2ConfigResponse.getConfig());
  }

  @Test
  public void testGetAllConfigs() {
    // upsert config with context1
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.of(CONTEXT_1), config1);

    // upsert config with context2
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.of(CONTEXT_2), config2);

    // verify getAllConfigs
    List<ContextSpecificConfig> contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1)
            .getContextSpecificConfigsList();
    assertEquals(2, contextSpecificConfigList.size());
    assertEquals(config2, contextSpecificConfigList.get(0).getConfig());
    assertEquals(CONTEXT_2, contextSpecificConfigList.get(0).getContext());
    assertEquals(config1, contextSpecificConfigList.get(1).getConfig());
    assertEquals(CONTEXT_1, contextSpecificConfigList.get(1).getContext());

    // delete first config and upsert it again so that its creation timestamp is greater than second
    // config
    deleteConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_1);
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.of(CONTEXT_1), config1);

    // verify getAllConfigs - order should be reversed this time
    contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1)
            .getContextSpecificConfigsList();
    assertEquals(2, contextSpecificConfigList.size());
    assertEquals(config1, contextSpecificConfigList.get(0).getConfig());
    assertEquals(CONTEXT_1, contextSpecificConfigList.get(0).getContext());
    assertEquals(config2, contextSpecificConfigList.get(1).getConfig());
    assertEquals(CONTEXT_2, contextSpecificConfigList.get(1).getContext());
  }

  @Test
  public void testDeleteConfig() {
    // upsert config with default or no context
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.empty(), config1);

    // upsert config with context
    upsertConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, Optional.of(CONTEXT_1), config2);

    // delete config with context
    ContextSpecificConfig deletedConfig =
        deleteConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_1).getDeletedConfig();
    assertEquals(config2, deletedConfig.getConfig());

    // get config with context should return default config
    Value config =
        getConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_1).getConfig();
    assertEquals(config1, config);

    // delete config with default context also
    deletedConfig =
        deleteConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1).getDeletedConfig();
    assertEquals(config1, deletedConfig.getConfig());

    // get config with context should return empty config
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> getConfig(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1, CONTEXT_1));

    assertEquals(Status.NOT_FOUND, exception.getStatus());
  }

  @Test
  void testDeleteConfigs() {
    // trying to bulk delete with empty list of configs should throw an error
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> deleteConfigs(Collections.emptyList(), TENANT_1));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());

    UpsertAllConfigsRequest upsertAllConfigsRequestForTenant1 =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_1, config1),
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_2, config2),
                    buildConfigToUpsert(RESOURCE_NAME_2, RESOURCE_NAMESPACE_2, CONTEXT_2, config2)))
            .build();
    UpsertAllConfigsRequest upsertAllConfigsRequestForTenant2 =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, CONTEXT_1, config1)))
            .build();
    upsertConfigs(upsertAllConfigsRequestForTenant1, TENANT_1);
    upsertConfigs(upsertAllConfigsRequestForTenant2, TENANT_2);

    DeleteConfigsRequest deleteConfigsRequest =
        DeleteConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToDelete(CONTEXT_1, RESOURCE_NAME_1, RESOURCE_NAMESPACE_1),
                    buildConfigToDelete(CONTEXT_2, RESOURCE_NAME_1, RESOURCE_NAMESPACE_1)))
            .build();
    List<ContextSpecificConfig> deletedContextSpecificConfigs =
        deleteConfigs(
                List.of(
                    buildConfigToDelete(CONTEXT_1, RESOURCE_NAME_1, RESOURCE_NAMESPACE_1),
                    buildConfigToDelete(CONTEXT_2, RESOURCE_NAME_1, RESOURCE_NAMESPACE_1)),
                TENANT_1)
            .getDeletedConfigsList();

    assertEquals(2, deletedContextSpecificConfigs.size());
    assertEquals(CONTEXT_1, deletedContextSpecificConfigs.get(0).getContext());
    assertEquals(config1, deletedContextSpecificConfigs.get(0).getConfig());
    assertEquals(CONTEXT_2, deletedContextSpecificConfigs.get(1).getContext());
    assertEquals(config2, deletedContextSpecificConfigs.get(1).getConfig());

    // There should be no configs for RESOURCE_NAME_1 and RESOURCE_NAMESPACE_1 for TENANT_1
    List<ContextSpecificConfig> contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_1)
            .getContextSpecificConfigsList();
    assertTrue(contextSpecificConfigList.isEmpty());

    // There should be 1 config for RESOURCE_NAME_2 and RESOURCE_NAMESPACE_2 for TENANT_1
    contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME_2, RESOURCE_NAMESPACE_2, TENANT_1)
            .getContextSpecificConfigsList();
    assertEquals(1, contextSpecificConfigList.size());
    assertEquals(CONTEXT_2, contextSpecificConfigList.get(0).getContext());
    assertEquals(config2, contextSpecificConfigList.get(0).getConfig());

    // There should be 1 config for RESOURCE_NAME_1 and RESOURCE_NAMESPACE_1 for TENANT_2
    contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME_1, RESOURCE_NAMESPACE_1, TENANT_2)
            .getContextSpecificConfigsList();
    assertEquals(1, contextSpecificConfigList.size());
    assertEquals(CONTEXT_1, contextSpecificConfigList.get(0).getContext());
    assertEquals(config1, contextSpecificConfigList.get(0).getConfig());
  }

  private UpsertConfigResponse upsertConfig(
      String resourceName,
      String resourceNamespace,
      String tenantId,
      Optional<String> context,
      Value config) {
    UpsertConfigRequest.Builder builder =
        UpsertConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .setConfig(config);
    if (context.isPresent()) {
      builder.setContext(context.get());
    }
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.upsertConfig(builder.build()));
  }

  private UpsertAllConfigsResponse upsertConfigs(
      UpsertAllConfigsRequest upsertAllConfigsRequest, String tenantId) {
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.upsertAllConfigs(upsertAllConfigsRequest));
  }

  private UpsertAllConfigsRequest.ConfigToUpsert buildConfigToUpsert(
      String resourceName, String resourceNamespace, String context, Value config) {
    return UpsertAllConfigsRequest.ConfigToUpsert.newBuilder()
        .setResourceName(resourceName)
        .setResourceNamespace(resourceNamespace)
        .setContext(context)
        .setConfig(config)
        .build();
  }

  private DeleteConfigsRequest.ConfigToDelete buildConfigToDelete(
      String context, String resourceName, String resourceNamespace) {
    return DeleteConfigsRequest.ConfigToDelete.newBuilder()
        .setContext(context)
        .setResourceName(resourceName)
        .setResourceNamespace(resourceNamespace)
        .build();
  }

  private GetConfigResponse getConfig(
      String resourceName, String resourceNamespace, String tenantId, String... contexts) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .addAllContexts(Arrays.asList(contexts))
            .build();
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.getConfig(getConfigRequest));
  }

  private GetAllConfigsResponse getAllConfigs(
      String resourceName, String resourceNamespace, String tenantId) {
    GetAllConfigsRequest request =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .build();
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.getAllConfigs(request));
  }

  private DeleteConfigResponse deleteConfig(
      String resourceName, String resourceNamespace, String tenantId) {
    DeleteConfigRequest request =
        DeleteConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .build();
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.deleteConfig(request));
  }

  private DeleteConfigsResponse deleteConfigs(
      List<DeleteConfigsRequest.ConfigToDelete> configsToDelete, String tenantId) {
    DeleteConfigsRequest deleteConfigsRequest =
        DeleteConfigsRequest.newBuilder().addAllConfigs(configsToDelete).build();
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.deleteConfigs(deleteConfigsRequest));
  }

  private DeleteConfigResponse deleteConfig(
      String resourceName, String resourceNamespace, String tenantId, String context) {
    DeleteConfigRequest request =
        DeleteConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .setContext(context)
            .build();
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.deleteConfig(request));
  }
}
