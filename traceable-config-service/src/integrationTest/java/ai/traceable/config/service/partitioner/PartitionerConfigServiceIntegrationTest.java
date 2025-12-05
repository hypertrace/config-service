package ai.traceable.config.service.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.typesafe.config.ConfigFactory;
import io.grpc.Deadline;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesDocumentStore;
import org.hypertrace.partitioner.config.service.v1.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class PartitionerConfigServiceIntegrationTest {

  private static PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub
      partitionerConfigServiceBlockingStub;

  private static final Collection PARTITIONER_PROFILES_COLLECTION =
      getPartitionerProfilesCollection();
  protected static final String SERVICE_NAME = "traceable-config-service";

  protected static InProcessGrpcChannelRegistry channelRegistry;

  @BeforeAll
  static void init() {
    IntegrationTestServerUtil.startServices(new String[] {SERVICE_NAME});
    channelRegistry = new InProcessGrpcChannelRegistry();
    partitionerConfigServiceBlockingStub =
        PartitionerConfigServiceGrpc.newBlockingStub(channelRegistry.forName(SERVICE_NAME))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @BeforeEach
  public void delete() {
    PARTITIONER_PROFILES_COLLECTION.deleteAll();
  }

  @AfterAll
  public static void teardown() {
    channelRegistry.shutdown(Deadline.after(1, TimeUnit.SECONDS));
    IntegrationTestServerUtil.shutdownServices();
  }

  @Test
  public void when2ProfilesArePut_thenOnQueryReturnTheExpectedProfile() {
    PartitionerProfile spanCountProfile =
        PartitionerProfile.newBuilder()
            .setName("spanCountProfile")
            .setPartitionKey("tenantId")
            .setDefaultGroupWeight(35)
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
            .setDefaultGroupWeight(35)
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

    PutPartitionerProfilesRequest request =
        PutPartitionerProfilesRequest.newBuilder()
            .addProfiles(spanCountProfile)
            .addProfiles(apiCountProfile)
            .build();

    PutPartitionerProfilesResponse response =
        new RequestContext()
            .call(() -> partitionerConfigServiceBlockingStub.putPartitionerProfiles(request));

    GetPartitionerProfileResponse spanCountProfileResponse =
        partitionerConfigServiceBlockingStub.getPartitionerProfile(
            GetPartitionerProfileRequest.newBuilder().setProfileName("spanCountProfile").build());

    GetPartitionerProfileResponse apiCountProfileResponse =
        partitionerConfigServiceBlockingStub.getPartitionerProfile(
            GetPartitionerProfileRequest.newBuilder().setProfileName("apiCountProfile").build());

    assertEquals(spanCountProfile, spanCountProfileResponse.getProfile());
    assertEquals(apiCountProfile, apiCountProfileResponse.getProfile());
  }

  @Test
  public void whenGetAllProfiles_thenReturnAllProfiles() {

    PartitionerProfile spanCountProfile =
        PartitionerProfile.newBuilder()
            .setName("spanCountProfile")
            .setPartitionKey("tenantId")
            .setDefaultGroupWeight(35)
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
            .setDefaultGroupWeight(35)
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
            .setDefaultGroupWeight(35)
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

    PutPartitionerProfilesRequest request =
        PutPartitionerProfilesRequest.newBuilder()
            .addProfiles(spanCountProfile)
            .addProfiles(apiCountProfile)
            .addProfiles(sessionsCountProfile)
            .build();

    partitionerConfigServiceBlockingStub.putPartitionerProfiles(request);

    GetPartitionerProfilesResponse partitionerProfilesResponse =
        partitionerConfigServiceBlockingStub.getPartitionerProfiles(
            GetPartitionerProfilesRequest.newBuilder().build());

    assertEquals(3, partitionerProfilesResponse.getProfilesCount());
  }

  @Test
  public void whenDeleteProfiles_thenExpectOthersToBeIntact() {

    PartitionerProfile spanCountProfile =
        PartitionerProfile.newBuilder()
            .setName("spanCountProfile")
            .setPartitionKey("tenantId")
            .setDefaultGroupWeight(35)
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
            .setDefaultGroupWeight(35)
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
            .setDefaultGroupWeight(35)
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

    PutPartitionerProfilesRequest request =
        PutPartitionerProfilesRequest.newBuilder()
            .addProfiles(spanCountProfile)
            .addProfiles(apiCountProfile)
            .addProfiles(sessionsCountProfile)
            .build();

    partitionerConfigServiceBlockingStub.putPartitionerProfiles(request);

    partitionerConfigServiceBlockingStub.deletePartitionerProfiles(
        DeletePartitionerProfilesRequest.newBuilder()
            .addProfileNames("spanCountProfile")
            .addProfileNames("sessionsCountProfile")
            .build());

    GetPartitionerProfilesResponse partitionerProfilesResponse =
        partitionerConfigServiceBlockingStub.getPartitionerProfiles(
            GetPartitionerProfilesRequest.newBuilder().build());

    assertFalse(
        partitionerProfilesResponse.getProfilesList().stream()
            .anyMatch(
                partitionerProfile ->
                    partitionerProfile.getName().equals("spanCountProfile")
                        || partitionerProfile.getName().equals("sessionsCountProfile")));
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
