package org.hypertrace.partitioner.config.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;
import org.hypertrace.partitioner.config.service.v1.PutPartitionerProfilesRequest;
import org.junit.jupiter.api.Test;

class PartitionerConfigServiceRequestValidatorTest {

  @Test
  public void testProfileNameExists() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(PartitionerProfile.newBuilder().build())
                        .build()));
  }

  @Test
  public void testDefaultGroupWeightIsValid() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(
                            PartitionerProfile.newBuilder().setName("spansCountProfile").build())
                        .build()));
  }

  @Test
  public void testProfilesExist() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () -> underTest.validateOrThrow(PutPartitionerProfilesRequest.newBuilder().build()));
  }

  @Test
  public void testPartitionGroupsExist() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();
assertDoesNotThrow(
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(
                            PartitionerProfile.newBuilder()
                                .setName("spansCountProfile")
                                .setDefaultGroupWeight(25)
                                .setPartitionKey("tenant_id")
                                .build())
                        .build()));
  }

  @Test
  public void testProfilePartitionKeyExists() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(
                            PartitionerProfile.newBuilder()
                                .setName("spansCountProfile")
                                .setDefaultGroupWeight(25)
                                .build())
                        .build()));
  }

  @Test
  public void testPartitionGroupNameExists() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    PartitionerProfile spanCountProfile =
        PartitionerProfile.newBuilder()
            .setName("spanCountProfile")
            .setPartitionKey("tenantId")
            .setDefaultGroupWeight(35)
            .addAllGroups(
                List.of(
                    PartitionerGroup.newBuilder()
                        .setWeight(25)
                        .addAllMemberIds(List.of("tenant1", "tenant2"))
                        .build(),
                    PartitionerGroup.newBuilder()
                        .setName("group2")
                        .setWeight(50)
                        .addAllMemberIds(List.of("tenant3", "tenant4"))
                        .build()))
            .build();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(spanCountProfile)
                        .build()));
  }

  @Test
  public void testPartitionGroupMembersExists() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

    PartitionerProfile spanCountProfile =
        PartitionerProfile.newBuilder()
            .setName("spanCountProfile")
            .setPartitionKey("tenantId")
            .setDefaultGroupWeight(35)
            .addAllGroups(
                List.of(
                    PartitionerGroup.newBuilder().setName("group1").setWeight(25).build(),
                    PartitionerGroup.newBuilder()
                        .setName("group2")
                        .setWeight(50)
                        .addAllMemberIds(List.of("tenant3", "tenant4"))
                        .build()))
            .build();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(spanCountProfile)
                        .build()));
  }

  @Test
  public void testPartitionGroupMembersAreUniqueExists() {
    PartitionerConfigServiceRequestValidator underTest =
        new PartitionerConfigServiceRequestValidator();

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
                        .addAllMemberIds(List.of("tenant5", "tenant4"))
                        .build(),
                    PartitionerGroup.newBuilder()
                        .setName("group2")
                        .setWeight(50)
                        .addAllMemberIds(List.of("tenant3", "tenant4"))
                        .build()))
            .build();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () ->
                underTest.validateOrThrow(
                    PutPartitionerProfilesRequest.newBuilder()
                        .addProfiles(spanCountProfile)
                        .build()));
  }
}
