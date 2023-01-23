package org.hypertrace.tenantpartitioning.config.service;

// import org.hypertrace.tenantpartitioning.config.service.v1.;

import io.grpc.Status;
import org.hypertrace.tenantpartitioning.config.service.v1.*;

public class TenantPartitioningConfigServiceRequestValidator {

  public void validateOrThrow(
      GetTenantPartitionGroupConfigsRequest request) { // nothing to validate
  }

  public void validateOrThrow(CreateTenantPartitionGroupConfigRequest request) {
    validateNewTenantPartitionConfigOrThrow(request.getConfig());
  }

  public void validateOrThrow(UpdateTenantPartitionGroupConfigRequest request) {
    validateTenantPartitionConfigOrThrow(request.getConfig());
  }

  public void validateOrThrow(DeleteTenantPartitionGroupConfigRequest request) {
    if (request.getId().isBlank()) {
      throw Status.INVALID_ARGUMENT.withDescription("id can't be empty").asRuntimeException();
    }
  }

  private void validateTenantPartitionConfigOrThrow(TenantPartitionGroupConfig config) {
    if (config == null) {
      throw Status.INVALID_ARGUMENT.withDescription("Config can't be empty").asRuntimeException();
    }

    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }

    if (config.getMemberTenantIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }

  private void validateNewTenantPartitionConfigOrThrow(NewTenantPartitionGroupConfig config) {
    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }

    if (config.getMemberTenantIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }

  private void validateTenantPartitionConfigOrThrow(TenantPartitionGroupConfigForUpdate config) {

    if (config.getMemberTenantIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }
}
