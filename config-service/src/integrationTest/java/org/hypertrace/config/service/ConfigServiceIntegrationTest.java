package org.hypertrace.config.service;

import static org.hypertrace.config.service.IntegrationTestUtils.getConfigValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest.ConfigToUpsert;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for ConfigService */
public class ConfigServiceIntegrationTest {

  private static final String RESOURCE_NAME = "foo";
  private static final String RESOURCE_NAMESPACE = "bar";
  private static final String TENANT_1 = "tenant1";
  private static final String TENANT_2 = "tenant2";
  private static final String CONTEXT_1 = "ctx1";
  private static final String CONTEXT_2 = "ctx2";
  private static final String DATA_STORE_COLLECTION = "configurations";
  private static final Collection CONFIGURATIONS_COLLECTION = getConfigurationsCollection();

  private static ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private static Value config1;
  private static Value config2;
  private static Value config3;

  @BeforeAll
  public static void setup() throws IOException {
    System.out.println("Starting Config Service E2E Test");
    IntegrationTestServerUtil.startServices(new String[] {"config-service"});

    Channel channel = ManagedChannelBuilder.forAddress("localhost", 50101).usePlaintext().build();

    configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    config1 = getConfigValue("config1.yaml");
    config2 = getConfigValue("config2.yaml");
    config3 = getConfigValue("config3.yaml");
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
  }

  // Need to delete the collection after each test for stateless integration testing
  @AfterEach
  void delete() {
    CONFIGURATIONS_COLLECTION.deleteAll();
  }

  @Test
  public void testUpsertConfig() {
    // upsert first version of config for tenant-1
    UpsertConfigResponse upsertConfigResponse =
        upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.empty(), config1);
    assertEquals(config1, upsertConfigResponse.getConfig());

    // upsert second version of config for tenant-1
    upsertConfigResponse =
        upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.empty(), config2);
    assertEquals(config2, upsertConfigResponse.getConfig());

    // test across tenants - upsert first version of config for tenant-2
    upsertConfigResponse =
        upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_2, Optional.empty(), config1);
    assertEquals(config1, upsertConfigResponse.getConfig());
  }

  @Test
  public void testUpsertAllConfigs() {
    String context1 = "context1";
    String context2 = "context2";
    UpsertAllConfigsRequest upsertAllConfigsRequest =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(context1, config1), buildConfigToUpsert(context2, config2)))
            .build();
    UpsertAllConfigsResponse upsertAllConfigsResponse =
        upsertConfigs(upsertAllConfigsRequest, TENANT_1);
    assertEquals(2, upsertAllConfigsResponse.getUpsertedConfigsCount());
    UpsertAllConfigsResponse.UpsertedConfig upsertedConfig1 =
        upsertAllConfigsResponse.getUpsertedConfigs(1);
    assertEquals(context1, upsertedConfig1.getContext());
    assertEquals(config1, upsertedConfig1.getConfig());
    assertFalse(upsertedConfig1.hasPrevConfig());
    UpsertAllConfigsResponse.UpsertedConfig upsertedConfig2 =
        upsertAllConfigsResponse.getUpsertedConfigs(0);
    assertEquals(context2, upsertedConfig2.getContext());
    assertEquals(config2, upsertedConfig2.getConfig());
    assertFalse(upsertedConfig2.hasPrevConfig());

    // Swap configs between contexts as an update
    upsertAllConfigsRequest =
        UpsertAllConfigsRequest.newBuilder()
            .addAllConfigs(
                List.of(
                    buildConfigToUpsert(context1, config2), buildConfigToUpsert(context2, config1)))
            .build();
    upsertAllConfigsResponse = upsertConfigs(upsertAllConfigsRequest, TENANT_1);
    assertEquals(2, upsertAllConfigsResponse.getUpsertedConfigsCount());
    upsertedConfig1 = upsertAllConfigsResponse.getUpsertedConfigs(0);
    assertEquals(context2, upsertedConfig1.getContext());
    assertEquals(config1, upsertedConfig1.getConfig());
    assertEquals(config2, upsertedConfig1.getPrevConfig());
    upsertedConfig2 = upsertAllConfigsResponse.getUpsertedConfigs(1);
    assertEquals(context1, upsertedConfig2.getContext());
    assertEquals(config2, upsertedConfig2.getConfig());
    assertEquals(config1, upsertedConfig2.getPrevConfig());
  }

  @Test
  public void testGetConfig() {
    // upsert config with default or no context
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.empty(), config1);

    // upsert config with context
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.of(CONTEXT_1), config2);

    // get default config (without context)
    GetConfigResponse getDefaultConfigResponse =
        getConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1);
    assertEquals(config1, getDefaultConfigResponse.getConfig());

    // get config with context as CONTEXT_1 - should merge the first two configs(which is config3)
    GetConfigResponse getContext1ConfigResponse =
        getConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_1);
    assertEquals(config3, getContext1ConfigResponse.getConfig());

    // get config with context as CONTEXT_2 - should return the config with default context
    GetConfigResponse getContext2ConfigResponse =
        getConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_2);
    assertEquals(config1, getContext2ConfigResponse.getConfig());
  }

  @Test
  public void testGetAllConfigs() {
    // upsert config with context1
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.of(CONTEXT_1), config1);

    // upsert config with context2
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.of(CONTEXT_2), config2);

    // verify getAllConfigs
    List<ContextSpecificConfig> contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1).getContextSpecificConfigsList();
    assertEquals(2, contextSpecificConfigList.size());
    assertEquals(config2, contextSpecificConfigList.get(0).getConfig());
    assertEquals(CONTEXT_2, contextSpecificConfigList.get(0).getContext());
    assertEquals(config1, contextSpecificConfigList.get(1).getConfig());
    assertEquals(CONTEXT_1, contextSpecificConfigList.get(1).getContext());

    // delete first config and upsert it again so that its creation timestamp is greater than second
    // config
    deleteConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_1);
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.of(CONTEXT_1), config1);

    // verify getAllConfigs - order should be reversed this time
    contextSpecificConfigList =
        getAllConfigs(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1).getContextSpecificConfigsList();
    assertEquals(2, contextSpecificConfigList.size());
    assertEquals(config1, contextSpecificConfigList.get(0).getConfig());
    assertEquals(CONTEXT_1, contextSpecificConfigList.get(0).getContext());
    assertEquals(config2, contextSpecificConfigList.get(1).getConfig());
    assertEquals(CONTEXT_2, contextSpecificConfigList.get(1).getContext());
  }

  @Test
  public void testDeleteConfig() {
    // upsert config with default or no context
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.empty(), config1);

    // upsert config with context
    upsertConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, Optional.of(CONTEXT_1), config2);

    // delete config with context
    ContextSpecificConfig deletedConfig =
        deleteConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_1).getDeletedConfig();
    assertEquals(config2, deletedConfig.getConfig());

    // get config with context should return default config
    Value config = getConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_1).getConfig();
    assertEquals(config1, config);

    // delete config with default context also
    deletedConfig = deleteConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1).getDeletedConfig();
    assertEquals(config1, deletedConfig.getConfig());

    // get config with context should return empty config
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> getConfig(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_1, CONTEXT_1));

    assertEquals(Status.NOT_FOUND, exception.getStatus());
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
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId, () -> configServiceBlockingStub.upsertConfig(builder.build()));
  }

  private UpsertAllConfigsResponse upsertConfigs(
      UpsertAllConfigsRequest upsertAllConfigsRequest, String tenantId) {
    return RequestContext.forTenantId(tenantId)
        .call(() -> configServiceBlockingStub.upsertAllConfigs(upsertAllConfigsRequest));
  }

  private ConfigToUpsert buildConfigToUpsert(String context, Value config) {
    return ConfigToUpsert.newBuilder()
        .setResourceName(RESOURCE_NAME)
        .setResourceNamespace(RESOURCE_NAMESPACE)
        .setContext(context)
        .setConfig(config)
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
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId, () -> configServiceBlockingStub.getConfig(getConfigRequest));
  }

  private GetAllConfigsResponse getAllConfigs(
      String resourceName, String resourceNamespace, String tenantId) {
    GetAllConfigsRequest request =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId, () -> configServiceBlockingStub.getAllConfigs(request));
  }

  private DeleteConfigResponse deleteConfig(
      String resourceName, String resourceNamespace, String tenantId) {
    DeleteConfigRequest request =
        DeleteConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId, () -> configServiceBlockingStub.deleteConfig(request));
  }

  private DeleteConfigResponse deleteConfig(
      String resourceName, String resourceNamespace, String tenantId, String context) {
    DeleteConfigRequest request =
        DeleteConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(resourceNamespace)
            .setContext(context)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId, () -> configServiceBlockingStub.deleteConfig(request));
  }

  private static Collection getConfigurationsCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(DATA_STORE_COLLECTION);
  }
}
