package org.hypertrace.config.service.tenantpartitioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionConfigDocumentStore;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupConfigKey;
import org.hypertrace.tenantpartitioning.config.service.v1.CreateTenantPartitionGroupConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.CreateTenantPartitionGroupConfigResponse;
import org.hypertrace.tenantpartitioning.config.service.v1.DeleteTenantPartitionGroupConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.DeleteTenantPartitionGroupConfigResponse;
import org.hypertrace.tenantpartitioning.config.service.v1.GetTenantPartitionGroupConfigsRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.GetTenantPartitionGroupConfigsResponse;
import org.hypertrace.tenantpartitioning.config.service.v1.NewTenantPartitionGroupConfig;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfig;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfigForUpdate;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitioningConfigServiceGrpc;
import org.hypertrace.tenantpartitioning.config.service.v1.UpdateTenantPartitionGroupConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.UpdateTenantPartitionGroupConfigResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TenantPartitioningConfigServiceIntegrationTest {

  private static TenantPartitioningConfigServiceGrpc.TenantPartitioningConfigServiceBlockingStub
      tenantPartitionConfigServiceBlockingStub;

  private static final Collection TENANT_PARTITION_CONFIG_COLLECTION =
      getTenantPartitionConfigCollection();

  protected static ManagedChannel globalConfigInternalChannel;

  @BeforeAll
  static void init() {
    IntegrationTestServerUtil.startServices(new String[] {"config-service"});
    globalConfigInternalChannel =
        ManagedChannelBuilder.forAddress("localhost", 50103).usePlaintext().build();
    tenantPartitionConfigServiceBlockingStub =
        TenantPartitioningConfigServiceGrpc.newBlockingStub(globalConfigInternalChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @BeforeEach
  public void delete() {
    TENANT_PARTITION_CONFIG_COLLECTION.deleteAll();
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
    globalConfigInternalChannel.shutdown();
  }

  @Test
  public void whenCollectionEmptyAndInsertNewConfig_thenExpectNewConfigToBeCreated() {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          CreateTenantPartitionGroupConfigRequest request =
              CreateTenantPartitionGroupConfigRequest.newBuilder()
                  .setConfig(
                      NewTenantPartitionGroupConfig.newBuilder()
                          .setGroupName("group1")
                          .setWeight(20)
                          .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
                          .build())
                  .build();
          CreateTenantPartitionGroupConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.createTenantPartitionGroupConfig(request);
          assertEquals("group1", response.getConfig().getGroupName());
        });
  }

  @Test
  public void whenGetAllConfigs_thenExpectAllToBeReturned() {

    TenantPartitionGroupConfig group1 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group1")
            .setWeight(20)
            .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
            .build();
    TenantPartitionGroupConfig group2 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group2")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant3", "tenant4"))
            .build();
    TenantPartitionGroupConfig group3 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group3")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant5", "tenant6"))
            .build();

    insertConfig(group1);
    insertConfig(group2);
    insertConfig(group3);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetTenantPartitionGroupConfigsResponse response =
              tenantPartitionConfigServiceBlockingStub.getTenantPartitionGroupConfigs(
                  GetTenantPartitionGroupConfigsRequest.newBuilder().build());
          assertEquals(3, response.getConfigsList().size());
        });
  }

  @Test
  public void whenDeleteAConfig_thenExpectAllButThisOneToBeReturned() {

    TenantPartitionGroupConfig group1 =
        TenantPartitionGroupConfig.newBuilder()
            .setId(new TenantPartitionGroupConfigKey("group1").toString())
            .setGroupName("group1")
            .setWeight(20)
            .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
            .build();
    TenantPartitionGroupConfig group2 =
        TenantPartitionGroupConfig.newBuilder()
            .setId(new TenantPartitionGroupConfigKey("group2").toString())
            .setGroupName("group2")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant3", "tenant4"))
            .build();
    TenantPartitionGroupConfig group3 =
        TenantPartitionGroupConfig.newBuilder()
            .setId(new TenantPartitionGroupConfigKey("group3").toString())
            .setGroupName("group3")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant5", "tenant6"))
            .build();

    insertConfig(group1);
    insertConfig(group2);
    insertConfig(group3);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          DeleteTenantPartitionGroupConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.deleteTenantPartitionGroupConfig(
                  DeleteTenantPartitionGroupConfigRequest.newBuilder()
                      .setId(group2.getId())
                      .build());
        });

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetTenantPartitionGroupConfigsResponse response =
              tenantPartitionConfigServiceBlockingStub.getTenantPartitionGroupConfigs(
                  GetTenantPartitionGroupConfigsRequest.newBuilder().build());
          assertEquals(2, response.getConfigsList().size());
          assertFalse(
              response.getConfigsList().stream()
                  .anyMatch(
                      tenantPartitionGroupConfig -> tenantPartitionGroupConfig.equals("group2")));
        });
  }

  @Test
  public void whenConfigUpdated_thenExpectUpdatedValues() {
    TenantPartitionGroupConfig group1 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group1")
            .setWeight(20)
            .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
            .build();

    TenantPartitionGroupConfig expected =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group1")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant1", "tenant4"))
            .build();

    insertConfig(group1);

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          UpdateTenantPartitionGroupConfigRequest request =
              UpdateTenantPartitionGroupConfigRequest.newBuilder()
                  .setConfig(
                      TenantPartitionGroupConfigForUpdate.newBuilder()
                          .setWeight(expected.getWeight())
                          .addAllMemberTenantIds(expected.getMemberTenantIdsList())
                          .build())
                  .build();
          UpdateTenantPartitionGroupConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.updateTenantPartitionGroupConfig(request);

          assertEquals(expected.getWeight(), response.getConfig().getWeight());
          assertEquals(
              expected.getMemberTenantIdsList(), response.getConfig().getMemberTenantIdsList());
        });
  }

  private void insertConfig(TenantPartitionGroupConfig config) {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          CreateTenantPartitionGroupConfigRequest request =
              CreateTenantPartitionGroupConfigRequest.newBuilder()
                  .setConfig(
                      NewTenantPartitionGroupConfig.newBuilder()
                          .setGroupName(config.getGroupName())
                          .setWeight(config.getWeight())
                          .addAllMemberTenantIds(config.getMemberTenantIdsList())
                          .build())
                  .build();
          CreateTenantPartitionGroupConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.createTenantPartitionGroupConfig(request);
        });
  }

  private static Collection getTenantPartitionConfigCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(
        TenantPartitionConfigDocumentStore.TENANT_ISOLATION_CONFIG_COLLECTION);
  }
}
