package org.hypertrace.config.service.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesDocumentStore;
import org.hypertrace.partitioner.config.service.v1.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionerConfigServiceIntegrationTest {

  private static PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub
      partitionerConfigServiceBlockingStub;

  private static final Collection PARTITIONER_PROFILES_COLLECTION =
      getPartitionerProfilesCollection();

  protected static ManagedChannel globalConfigInternalChannel;

  @BeforeAll
  static void init() {
    IntegrationTestServerUtil.startServices(new String[] {"config-service"});
    globalConfigInternalChannel =
        ManagedChannelBuilder.forAddress("localhost", 50103).usePlaintext().build();
    partitionerConfigServiceBlockingStub =
        PartitionerConfigServiceGrpc.newBlockingStub(globalConfigInternalChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @BeforeEach
  public void delete() {
    PARTITIONER_PROFILES_COLLECTION.deleteAll();
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
    globalConfigInternalChannel.shutdown();
  }

  @Test
  public void when2ProfilesArePut_thenOnQueryReturnTheExpectedProfile() {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          PartitionerProfile spanCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("spanCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(25)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(50)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PartitionerProfile apiCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("apiCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(35)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(55)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PutPartitionerConfigRequest request =
              PutPartitionerConfigRequest.newBuilder().setProfile(spanCountProfile).build();

          partitionerConfigServiceBlockingStub.putPartitionerConfig(request);

          partitionerConfigServiceBlockingStub.putPartitionerConfig(
              PutPartitionerConfigRequest.newBuilder().setProfile(apiCountProfile).build());

          GetPartitionerProfileResponse response =
              partitionerConfigServiceBlockingStub.getPartitionerProfile(
                  GetPartitionerProfileRequest.newBuilder().setProfile("spanCountProfile").build());
          PartitionerProfile actual = response.getProfile();
          assertEquals(spanCountProfile, actual);

          response =
              partitionerConfigServiceBlockingStub.getPartitionerProfile(
                  GetPartitionerProfileRequest.newBuilder().setProfile("apiCountProfile").build());
          actual = response.getProfile();
          assertEquals(apiCountProfile, actual);
        });
  }

  @Test
  public void whenGetAllProfiles_thenReturnAllProfiles() {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          PartitionerProfile spanCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("spanCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(25)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(50)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PartitionerProfile apiCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("apiCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(35)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(55)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PartitionerProfile sessionsCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("sessionsCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(25)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(70)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PutPartitionerConfigRequest request =
              PutPartitionerConfigRequest.newBuilder().setProfile(spanCountProfile).build();

          partitionerConfigServiceBlockingStub.putPartitionerConfig(request);

          partitionerConfigServiceBlockingStub.putPartitionerConfig(
              PutPartitionerConfigRequest.newBuilder().setProfile(apiCountProfile).build());

          partitionerConfigServiceBlockingStub.putPartitionerConfig(
              PutPartitionerConfigRequest.newBuilder().setProfile(sessionsCountProfile).build());

          GetAllPartitionerConfigResponse getAllPartitionerConfigResponse =
              partitionerConfigServiceBlockingStub.getAllPartitionerConfig(
                  GetAllPartitionerConfigRequest.newBuilder().build());
          assertEquals(3, getAllPartitionerConfigResponse.getProfilesCount());
        });
  }

  @Test
  public void whenDeleteAProfile_thenExpectOthersToBeIntact() {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        Collections.emptyMap(),
        () -> {
          PartitionerProfile spanCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("spanCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(25)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(50)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PartitionerProfile apiCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("apiCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(35)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(55)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PartitionerProfile sessionsCountProfile =
              PartitionerProfile.newBuilder()
                  .setName("sessionsCountProfile")
                  .setPartitionKey("tenantId")
                  .addAllGroups(
                      List.of(
                          PartitionerGroup.newBuilder()
                              .setName("group1")
                              .setWeight(25)
                              .addAllMemberIds(List.of("tenant1", "tenant2"))
                              .build(),
                          PartitionerGroup.newBuilder()
                              .setName("group2")
                              .setWeight(70)
                              .addAllMemberIds(List.of("tenant3", "tenant4"))
                              .build()))
                  .build();

          PutPartitionerConfigRequest request =
              PutPartitionerConfigRequest.newBuilder().setProfile(spanCountProfile).build();

          partitionerConfigServiceBlockingStub.putPartitionerConfig(request);

          partitionerConfigServiceBlockingStub.putPartitionerConfig(
              PutPartitionerConfigRequest.newBuilder().setProfile(apiCountProfile).build());

          partitionerConfigServiceBlockingStub.putPartitionerConfig(
              PutPartitionerConfigRequest.newBuilder().setProfile(sessionsCountProfile).build());

          partitionerConfigServiceBlockingStub.deletePartitionerConfig(
              DeletePartitionerConfigRequest.newBuilder().setProfile("spanCountProfile").build());

          GetAllPartitionerConfigResponse getAllPartitionerConfigResponse =
              partitionerConfigServiceBlockingStub.getAllPartitionerConfig(
                  GetAllPartitionerConfigRequest.newBuilder().build());
          assertEquals(2, getAllPartitionerConfigResponse.getProfilesCount());

          assertFalse(
              getAllPartitionerConfigResponse.getProfilesList().stream()
                  .anyMatch(
                      partitionerProfile ->
                          partitionerProfile.getName().equals("spanCountProfile")));
        });
  }

  private static Collection getPartitionerProfilesCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(PartitionerProfilesDocumentStore.PARTITIONER_PROFILES);
  }
}
