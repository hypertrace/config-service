package org.hypertrace.tenantpartitioning.config.service;

// import org.hypertrace.tenantpartitioning.config.service.v1.;

import io.grpc.Status;
import org.hypertrace.tenantpartitioning.config.service.v1.*;

public class TenantPartitioningConfigServiceRequestValidator {

  public void validateOrThrow(
      GetTenantPartitionGroupsConfigRequest request) { // nothing to validate
  }

  public void validateOrThrow(PutTenantPartitionGroupsConfigRequest request) {
    // validateNewTenantPartitionConfigOrThrow(request.getConfig());
  }

  public void validateOrThrow(DeleteTenantPartitionGroupsConfigRequest request) {}

  private void validateTenantPartitionConfigOrThrow(TenantPartitionGroupConfig config) {
    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }

    if (config.getMemberTenantIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }
}
