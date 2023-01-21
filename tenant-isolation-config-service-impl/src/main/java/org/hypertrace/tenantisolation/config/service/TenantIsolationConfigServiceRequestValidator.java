package org.hypertrace.tenantisolation.config.service;

import io.grpc.Status;
import org.hypertrace.tenantisolation.config.service.v1.*;

public class TenantIsolationConfigServiceRequestValidator {
  public void validateOrThrow(GetAllTenantIsolationGroupConfigsRequest request) {
    // nothing to validate
  }

  public void validateOrThrow(CreateTenantIsolationGroupConfigRequest request) {
    validateNewTenantIsolationConfigOrThrow(request.getConfig());
  }

  public void validateOrThrow(UpdateTenantIsolationGroupConfigRequest request) {
    validateTenantIsolationConfigOrThrow(request.getConfig());
  }

  public void validateOrThrow(DeleteTenantIsolationGroupConfigRequest request) {
    if (request.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }
  }

  private void validateTenantIsolationConfigOrThrow(TenantIsolationGroupConfig config) {
    if (config == null) {
      throw Status.INVALID_ARGUMENT.withDescription("Config can't be empty").asRuntimeException();
    }
    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }

    if (config.getMembersList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }

  private void validateNewTenantIsolationConfigOrThrow(NewTenantIsolationGroupConfig config) {
    if (config == null) {
      throw Status.INVALID_ARGUMENT.withDescription("Config can't be empty").asRuntimeException();
    }
    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }

    if (config.getMembersList().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("members can't be empty").asRuntimeException();
    }
  }
}
