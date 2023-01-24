package org.hypertrace.config.service.tenantpartitioning;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupsConfigDocumentStore;
import org.hypertrace.tenantpartitioning.config.service.v1.DeleteTenantPartitionGroupsConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.GetTenantPartitionGroupsConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.GetTenantPartitionGroupsConfigResponse;
import org.hypertrace.tenantpartitioning.config.service.v1.PutTenantPartitionGroupsConfigRequest;
import org.hypertrace.tenantpartitioning.config.service.v1.PutTenantPartitionGroupsConfigResponse;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfig;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupsConfig;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitioningConfigServiceGrpc;
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
          PutTenantPartitionGroupsConfigRequest request =
              PutTenantPartitionGroupsConfigRequest.newBuilder()
                  .setConfig(
                      TenantPartitionGroupsConfig.newBuilder()
                          .addConfigs(
                              TenantPartitionGroupConfig.newBuilder()
                                  .setGroupName("group1")
                                  .setWeight(20)
                                  .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
                                  .build())
                          .addConfigs(
                              TenantPartitionGroupConfig.newBuilder()
                                  .setGroupName("group2")
                                  .setWeight(40)
                                  .addAllMemberTenantIds(List.of("tenant3", "tenant4"))
                                  .build())
                          .build())
                  .build();

          PutTenantPartitionGroupsConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.putTenantPartitionGroupsConfig(request);
          assertEquals(2, response.getConfig().getConfigsCount());
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

    insertConfig(
        TenantPartitionGroupsConfig.newBuilder()
            .addConfigs(group1)
            .addConfigs(group2)
            .addConfigs(group3)
            .build());

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetTenantPartitionGroupsConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.getTenantPartitionGroupsConfig(
                  GetTenantPartitionGroupsConfigRequest.newBuilder().build());
          assertEquals(3, response.getConfig().getConfigsCount());
        });
  }

  @Test
  public void whenMultiplePuts_thenExpectTheLatest() {
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

    insertConfig(
        TenantPartitionGroupsConfig.newBuilder()
            .addConfigs(group1)
            .addConfigs(group2)
            .addConfigs(group3)
            .build());

    TenantPartitionGroupConfig group4 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group1")
            .setWeight(20)
            .addAllMemberTenantIds(List.of("tenant1", "tenant2"))
            .build();
    TenantPartitionGroupConfig group5 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group2")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant3", "tenant4"))
            .build();
    TenantPartitionGroupConfig group6 =
        TenantPartitionGroupConfig.newBuilder()
            .setGroupName("group3")
            .setWeight(40)
            .addAllMemberTenantIds(List.of("tenant5", "tenant6"))
            .build();

    insertConfig(
        TenantPartitionGroupsConfig.newBuilder()
            .addConfigs(group4)
            .addConfigs(group5)
            .addConfigs(group6)
            .build());

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetTenantPartitionGroupsConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.getTenantPartitionGroupsConfig(
                  GetTenantPartitionGroupsConfigRequest.newBuilder().build());
          assertEquals(3, response.getConfig().getConfigsCount());
          assertEquals(List.of(group4, group5, group6), response.getConfig().getConfigsList());
        });
  }

  @Test
  public void whenDeleteConfig_thenExpectAllToBeDeleted() {

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

    insertConfig(
        TenantPartitionGroupsConfig.newBuilder()
            .addConfigs(group1)
            .addConfigs(group2)
            .addConfigs(group3)
            .build());

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          tenantPartitionConfigServiceBlockingStub.deleteTenantPartitionGroupsConfig(
              DeleteTenantPartitionGroupsConfigRequest.newBuilder().build());
        });

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          GetTenantPartitionGroupsConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.getTenantPartitionGroupsConfig(
                  GetTenantPartitionGroupsConfigRequest.newBuilder().build());
          assertEquals(0, response.getConfig().getConfigsCount());
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

    insertConfig(TenantPartitionGroupsConfig.newBuilder().addConfigs(group1).build());

    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          PutTenantPartitionGroupsConfigResponse response =
              tenantPartitionConfigServiceBlockingStub.putTenantPartitionGroupsConfig(
                  PutTenantPartitionGroupsConfigRequest.newBuilder()
                      .setConfig(
                          TenantPartitionGroupsConfig.newBuilder().addConfigs(expected).build())
                      .build());

          assertEquals(expected.getWeight(), response.getConfig().getConfigs(0).getWeight());
          assertEquals(
              expected.getMemberTenantIdsList(),
              response.getConfig().getConfigs(0).getMemberTenantIdsList());
        });
  }

  private void insertConfig(TenantPartitionGroupsConfig config) {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          PutTenantPartitionGroupsConfigRequest request =
              PutTenantPartitionGroupsConfigRequest.newBuilder().setConfig(config).build();
          tenantPartitionConfigServiceBlockingStub.putTenantPartitionGroupsConfig(request);
        });
  }

  private static Collection getTenantPartitionConfigCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(
        TenantPartitionGroupsConfigDocumentStore.TENANT_ISOLATION_CONFIG_COLLECTION);
  }
}
