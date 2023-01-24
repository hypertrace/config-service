package org.hypertrace.tenantpartitioning.config.service;

import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupsConfigStore;
import org.hypertrace.tenantpartitioning.config.service.v1.*;

@Slf4j
public class TenantPartitioningConfigServiceImpl
    extends TenantPartitioningConfigServiceGrpc.TenantPartitioningConfigServiceImplBase {

  private final TenantPartitionGroupsConfigStore tenantIsolationConfigStore;
  private final TenantPartitioningConfigServiceRequestValidator validator;

  @Inject
  public TenantPartitioningConfigServiceImpl(
      TenantPartitionGroupsConfigStore tenantIsolationConfigStore,
      TenantPartitioningConfigServiceRequestValidator validator) {
    this.tenantIsolationConfigStore = tenantIsolationConfigStore;
    this.validator = validator;
  }

  public void getTenantPartitionGroupsConfig(
      GetTenantPartitionGroupsConfigRequest request,
      StreamObserver<GetTenantPartitionGroupsConfigResponse> responseObserver) {
    try {
      Optional<TenantPartitionGroupsConfig> config = tenantIsolationConfigStore.getConfig();
      if (config.isPresent()) {
        responseObserver.onNext(
            GetTenantPartitionGroupsConfigResponse.newBuilder().setConfig(config.get()).build());
      } else {
        responseObserver.onNext(GetTenantPartitionGroupsConfigResponse.newBuilder().build());
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("getTenantPartitionGroupsConfig failed with error ", e);
      responseObserver.onError(e);
    }
  }

  /** */
  public void putTenantPartitionGroupsConfig(
      PutTenantPartitionGroupsConfigRequest request,
      StreamObserver<PutTenantPartitionGroupsConfigResponse> responseObserver) {

    try {
      TenantPartitionGroupsConfig config =
          this.tenantIsolationConfigStore.putConfig(request.getConfig());
      responseObserver.onNext(
          PutTenantPartitionGroupsConfigResponse.newBuilder().setConfig(config).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("putTenantPartitionGroupsConfig failed for request {} with error ", request, e);
      responseObserver.onError(e);
    }
  }

  /** */
  public void deleteTenantPartitionGroupsConfig(
      DeleteTenantPartitionGroupsConfigRequest request,
      StreamObserver<DeleteTenantPartitionGroupsConfigResponse> responseObserver) {
    try {
      this.tenantIsolationConfigStore.deleteConfig();
      responseObserver.onNext(DeleteTenantPartitionGroupsConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("deleteTenantPartitionGroupsConfig failed with error ", e);
      responseObserver.onError(e);
    }
  }
}
