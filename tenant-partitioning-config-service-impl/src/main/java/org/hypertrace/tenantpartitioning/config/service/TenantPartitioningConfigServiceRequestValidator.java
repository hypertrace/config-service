package org.hypertrace.tenantpartitioning.config.service;

import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.tenantpartitioning.config.service.v1.*;

public class TenantPartitioningConfigServiceRequestValidator {

  public void validateOrThrow(GetTenantPartitionGroupsConfigRequest request) {
    // nothing to validate
  }

  public void validateOrThrow(PutTenantPartitionGroupsConfigRequest request) {
    validateTenantPartitionGroupsConfigOrThrow(request.getConfig());
  }

  private void validateTenantPartitionGroupsConfigOrThrow(TenantPartitionGroupsConfig config) {
    config.getConfigsList().forEach(this::validateTenantPartitionConfigOrThrow);
    // check if member_tenant_ids are disjoint across groups
    Map<String, String> tenantId2GroupName = new HashMap<>();
    for (TenantPartitionGroupConfig cfg : config.getConfigsList()) {
      for (String tenantId : cfg.getMemberTenantIdsList()) {
        if (tenantId2GroupName.containsKey(tenantId)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "tenantId {%s} already member of group {%s}",
                      tenantId, tenantId2GroupName.get(tenantId)))
              .asRuntimeException();
        } else {
          tenantId2GroupName.put(tenantId, cfg.getGroupName());
        }
      }
    }
  }

  private void validateTenantPartitionConfigOrThrow(TenantPartitionGroupConfig config) {
    if (config.getGroupName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("groupName can't be empty")
          .asRuntimeException();
    }
  }
}
