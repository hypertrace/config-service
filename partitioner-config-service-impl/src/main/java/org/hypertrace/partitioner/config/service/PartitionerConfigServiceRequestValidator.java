package org.hypertrace.partitioner.config.service;

import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.partitioner.config.service.v1.*;

public class PartitionerConfigServiceRequestValidator {

  public void validateOrThrow(GetPartitionerProfileRequest request) {
    if (request.getProfileName().isBlank()) {
      throw Status.INVALID_ARGUMENT.withDescription("profile cannot be empty").asRuntimeException();
    }
  }

  public void validateOrThrow(PutPartitionerProfilesRequest request) {
    request
        .getProfilesList()
        .forEach(
            profile -> {
              if (profile.getName().isBlank()) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("profile cannot be empty")
                    .asRuntimeException();
              }
              if (profile.getGroupsCount() == 0) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("partition_groups cannot be empty")
                    .asRuntimeException();
              }
              if (profile.getPartitionKey().isBlank()) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("partition key cannot be empty")
                    .asRuntimeException();
              }
              profile
                  .getGroupsList()
                  .forEach(
                      partitionerGroup -> {
                        if (partitionerGroup.getName().isBlank()) {
                          throw Status.INVALID_ARGUMENT
                              .withDescription("partition_group name cannot be empty")
                              .asRuntimeException();
                        }
                        if (partitionerGroup.getMemberIdsCount() == 0) {
                          throw Status.INVALID_ARGUMENT
                              .withDescription("partition_group memberIds cannot be empty")
                              .asRuntimeException();
                        }
                      });

              Map<String, String> memberId2PartitionerGroupName = new HashMap<>();
              profile
                  .getGroupsList()
                  .forEach(
                      partitionerGroup -> {
                        partitionerGroup
                            .getMemberIdsList()
                            .forEach(
                                memberId -> {
                                  if (memberId2PartitionerGroupName.containsKey(memberId)) {
                                    throw Status.INVALID_ARGUMENT
                                        .withDescription(
                                            String.format(
                                                "memberId {%s} already member of group {%s}",
                                                memberId,
                                                memberId2PartitionerGroupName.get(memberId)))
                                        .asRuntimeException();
                                  } else {
                                    memberId2PartitionerGroupName.put(
                                        memberId, partitionerGroup.getName());
                                  }
                                });
                      });
            });
  }

  public void validateOrThrow(DeletePartitionerProfilesRequest request) {
    request
        .getProfileList()
        .forEach(
            profile -> {
              if (profile.isBlank()) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("profile cannot be empty")
                    .asRuntimeException();
              }
            });
  }
}
