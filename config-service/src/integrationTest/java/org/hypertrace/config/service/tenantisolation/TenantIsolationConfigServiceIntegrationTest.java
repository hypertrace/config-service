package org.hypertrace.config.service.tenantisolation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.ConfigServiceIntegrationTestBase;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.tenantisolation.config.service.store.TenantIsolationConfigDocumentStore;
import org.hypertrace.tenantisolation.config.service.store.TenantIsolationGroupConfigDTO;
import org.hypertrace.tenantisolation.config.service.v1.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantIsolationConfigServiceIntegrationTest extends ConfigServiceIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(TenantIsolationConfigServiceIntegrationTest.class);
  private static TenantIsolationConfigServiceGrpc.TenantIsolationConfigServiceBlockingStub
      tenantIsolationConfigServiceBlockingStub;

  private static final Collection TENANT_ISOLATION_CONFIG_COLLECTION =
      getTenantIsolationConfigCollection();

  @BeforeAll
  static void init() {
    tenantIsolationConfigServiceBlockingStub =
        TenantIsolationConfigServiceGrpc.newBlockingStub(globalConfigInternalChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @BeforeEach
  public void delete() {
    TENANT_ISOLATION_CONFIG_COLLECTION.deleteAll();
  }

  @Test
  public void whenCollectionEmptyAndInsertNewConfig_thenExpectNewConfigToBeCreated() {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          CreateTenantIsolationGroupConfigRequest request =
              CreateTenantIsolationGroupConfigRequest.newBuilder()
                  .setConfig(
                      NewTenantIsolationGroupConfig.newBuilder()
                          .setGroupName("group1")
                          .setWeight(20)
                          .addAllMembers(List.of("tenant1", "tenant2"))
                          .build())
                  .build();
          CreateTenantIsolationGroupConfigResponse response =
              tenantIsolationConfigServiceBlockingStub.createTenantIsolationGroupConfig(request);
          assertEquals("group1", response.getConfig().getGroupName());
          logger.info("Created " + response.getConfig());
        });
  }

  @Test
  public void whenGetAllConfigs_thenExpectAllToBeReturned() {

    TenantIsolationGroupConfigDTO group1 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group1")
            .weight(20)
            .members(List.of("tenant1", "tenant2"))
            .build();
    TenantIsolationGroupConfigDTO group2 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group2")
            .weight(40)
            .members(List.of("tenant3", "tenant4"))
            .build();
    TenantIsolationGroupConfigDTO group3 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group3")
            .weight(40)
            .members(List.of("tenant5", "tenant6"))
            .build();

    insertConfig(group1);
    insertConfig(group2);
    insertConfig(group3);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetAllTenantIsolationGroupConfigsResponse response =
              tenantIsolationConfigServiceBlockingStub.getAllTenantIsolationGroupConfigs(
                  GetAllTenantIsolationGroupConfigsRequest.newBuilder().build());
          assertEquals(3, response.getConfigsList().size());
        });
  }

  @Test
  public void whenDeleteAConfig_thenExpectAllButThisOneToBeReturned() {

    TenantIsolationGroupConfigDTO group1 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group1")
            .weight(20)
            .members(List.of("tenant1", "tenant2"))
            .build();
    TenantIsolationGroupConfigDTO group2 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group2")
            .weight(40)
            .members(List.of("tenant3", "tenant4"))
            .build();
    TenantIsolationGroupConfigDTO group3 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group3")
            .weight(40)
            .members(List.of("tenant5", "tenant6"))
            .build();

    insertConfig(group1);
    insertConfig(group2);
    insertConfig(group3);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          DeleteTenantIsolationGroupConfigResponse response =
              tenantIsolationConfigServiceBlockingStub.deleteTenantIsolationGroupConfig(
                  DeleteTenantIsolationGroupConfigRequest.newBuilder()
                      .setGroupName("group2")
                      .build());
        });

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetAllTenantIsolationGroupConfigsResponse response =
              tenantIsolationConfigServiceBlockingStub.getAllTenantIsolationGroupConfigs(
                  GetAllTenantIsolationGroupConfigsRequest.newBuilder().build());
          assertEquals(2, response.getConfigsList().size());
          assertFalse(
              response.getConfigsList().stream()
                  .anyMatch(
                      tenantIsolationGroupConfig -> tenantIsolationGroupConfig.equals("group2")));
        });
  }

  @Test
  public void whenConfigUpdated_thenExpectUpdatedValues() {
    TenantIsolationGroupConfigDTO group1 =
        TenantIsolationGroupConfigDTO.builder()
            .groupName("group1")
            .weight(20)
            .members(List.of("tenant1", "tenant2"))
            .build();

    TenantIsolationGroupConfig expected =
        TenantIsolationGroupConfig.newBuilder()
            .setGroupName("group1")
            .setWeight(40)
            .addAllMembers(List.of("tenant1", "tenant4"))
            .build();

    insertConfig(group1);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          UpdateTenantIsolationGroupConfigRequest request =
              UpdateTenantIsolationGroupConfigRequest.newBuilder().setConfig(expected).build();
          UpdateTenantIsolationGroupConfigResponse response =
              tenantIsolationConfigServiceBlockingStub.updateTenantIsolationGroupConfig(request);

          assertEquals(expected.getGroupName(), response.getConfig().getGroupName());
          assertEquals(expected.getWeight(), response.getConfig().getWeight());
          assertEquals(expected.getMembersList(), response.getConfig().getMembersList());
        });
  }

  private void insertConfig(TenantIsolationGroupConfigDTO config) {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          CreateTenantIsolationGroupConfigRequest request =
              CreateTenantIsolationGroupConfigRequest.newBuilder()
                  .setConfig(
                      NewTenantIsolationGroupConfig.newBuilder()
                          .setGroupName(config.getGroupName())
                          .setWeight(config.getWeight())
                          .addAllMembers(config.getMembers())
                          .build())
                  .build();
          CreateTenantIsolationGroupConfigResponse response =
              tenantIsolationConfigServiceBlockingStub.createTenantIsolationGroupConfig(request);
          logger.info("Created " + response.getConfig());
        });
  }

  private static Collection getTenantIsolationConfigCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(
        TenantIsolationConfigDocumentStore.TENANT_ISOLATION_CONFIG_COLLECTION);
  }
}
