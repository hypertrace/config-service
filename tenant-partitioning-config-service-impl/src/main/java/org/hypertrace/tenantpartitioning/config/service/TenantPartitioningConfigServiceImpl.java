package org.hypertrace.tenantpartitioning.config.service;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionConfigStore;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupConfigKey;
import org.hypertrace.tenantpartitioning.config.service.v1.*;

@Slf4j
public class TenantPartitioningConfigServiceImpl
    extends TenantPartitioningConfigServiceGrpc.TenantPartitioningConfigServiceImplBase {

  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";

  private final TenantPartitionConfigStore tenantIsolationConfigStore;
  private final TenantPartitioningConfigServiceRequestValidator validator;

  @Inject
  public TenantPartitioningConfigServiceImpl(
      Config config,
      TenantPartitionConfigStore tenantIsolationConfigStore,
      TenantPartitioningConfigServiceRequestValidator validator) {
    this.tenantIsolationConfigStore = tenantIsolationConfigStore;
    this.validator = validator;
    Config cfg = config.getConfig(GENERIC_CONFIG_SERVICE);
    // this.tenantIsolationConfigStore = TenantPartitionConfigStoreProvider.getDocumentStore(cfg);
  }

  public void getTenantPartitionGroupConfigs(
      GetTenantPartitionGroupConfigsRequest request,
      StreamObserver<GetTenantPartitionGroupConfigsResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      List<TenantPartitionGroupConfig> configDTOs = tenantIsolationConfigStore.getAllGroupConfigs();
      List<TenantPartitionGroupConfig> configs =
          configDTOs.stream()
              .map(
                  TenantPartitionGroupConfigDTO ->
                      TenantPartitionGroupConfig.newBuilder()
                          .setGroupName(TenantPartitionGroupConfigDTO.getGroupName())
                          .setWeight(TenantPartitionGroupConfigDTO.getWeight())
                          .addAllMemberTenantIds(
                              TenantPartitionGroupConfigDTO.getMemberTenantIdsList())
                          .build())
              .collect(Collectors.toList());

      responseObserver.onNext(
          GetTenantPartitionGroupConfigsResponse.newBuilder().addAllConfigs(configs).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("getAllTenantPartitionGroupConfigs failed ", e);
      responseObserver.onError(e);
    }
  }

  public void createTenantPartitionGroupConfig(
      CreateTenantPartitionGroupConfigRequest request,
      StreamObserver<CreateTenantPartitionGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      TenantPartitionGroupConfig upsertedConfigDTO =
          tenantIsolationConfigStore.upsert(
              TenantPartitionGroupConfig.newBuilder()
                  .setId(
                      new TenantPartitionGroupConfigKey(request.getConfig().getGroupName())
                          .toString())
                  .setGroupName(request.getConfig().getGroupName())
                  .setWeight(request.getConfig().getWeight())
                  .addAllMemberTenantIds(request.getConfig().getMemberTenantIdsList())
                  .build());
      responseObserver.onNext(
          CreateTenantPartitionGroupConfigResponse.newBuilder()
              .setConfig(
                  TenantPartitionGroupConfig.newBuilder()
                      .setGroupName(upsertedConfigDTO.getGroupName())
                      .setWeight(upsertedConfigDTO.getWeight())
                      .addAllMemberTenantIds(upsertedConfigDTO.getMemberTenantIdsList())
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          String.format(
              "createTenantPartitionGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }

  public void updateTenantPartitionGroupConfig(
      UpdateTenantPartitionGroupConfigRequest request,
      StreamObserver<UpdateTenantPartitionGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      TenantPartitionGroupConfig upsertedConfigDTO =
          tenantIsolationConfigStore.upsert(
              TenantPartitionGroupConfig.newBuilder()
                  .addAllMemberTenantIds(request.getConfig().getMemberTenantIdsList())
                  .setWeight(request.getConfig().getWeight())
                  .build());
      responseObserver.onNext(
          UpdateTenantPartitionGroupConfigResponse.newBuilder()
              .setConfig(
                  TenantPartitionGroupConfig.newBuilder()
                      .setGroupName(upsertedConfigDTO.getGroupName())
                      .setWeight(upsertedConfigDTO.getWeight())
                      .addAllMemberTenantIds(upsertedConfigDTO.getMemberTenantIdsList())
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          String.format(
              "updateTenantPartitionGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }

  public void deleteTenantPartitionGroupConfig(
      DeleteTenantPartitionGroupConfigRequest request,
      StreamObserver<DeleteTenantPartitionGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      tenantIsolationConfigStore.delete(request.getId());
      responseObserver.onNext(DeleteTenantPartitionGroupConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          String.format(
              "deleteTenantPartitionGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }
}
