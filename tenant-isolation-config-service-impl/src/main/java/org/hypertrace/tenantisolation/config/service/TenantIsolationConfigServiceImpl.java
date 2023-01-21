package org.hypertrace.tenantisolation.config.service;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.tenantisolation.config.service.store.TenantIsolationConfigStore;
import org.hypertrace.tenantisolation.config.service.store.TenantIsolationConfigStoreProvider;
import org.hypertrace.tenantisolation.config.service.store.TenantIsolationGroupConfigDTO;
import org.hypertrace.tenantisolation.config.service.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantIsolationConfigServiceImpl
    extends TenantIsolationConfigServiceGrpc.TenantIsolationConfigServiceImplBase {

  private static final Logger logger =
      LoggerFactory.getLogger(TenantIsolationConfigServiceImpl.class);

  private static final String GENERIC_CONFIG_SERVICE = "generic.config.service";

  private TenantIsolationConfigServiceRequestValidator validator =
      new TenantIsolationConfigServiceRequestValidator();

  private TenantIsolationConfigStore tenantIsolationConfigStore;

  @Inject
  public TenantIsolationConfigServiceImpl(Config config) {
    Config cfg = config.getConfig(GENERIC_CONFIG_SERVICE);
    this.tenantIsolationConfigStore = TenantIsolationConfigStoreProvider.getDocumentStore(cfg);
  }

  public void getAllTenantIsolationGroupConfigs(
      GetAllTenantIsolationGroupConfigsRequest request,
      StreamObserver<GetAllTenantIsolationGroupConfigsResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      List<TenantIsolationGroupConfigDTO> configDTOs =
          tenantIsolationConfigStore.getAllGroupConfigs();
      List<TenantIsolationGroupConfig> configs =
          configDTOs.stream()
              .map(
                  tenantIsolationGroupConfigDTO ->
                      TenantIsolationGroupConfig.newBuilder()
                          .setGroupName(tenantIsolationGroupConfigDTO.getGroupName())
                          .setWeight(tenantIsolationGroupConfigDTO.getWeight())
                          .addAllMembers(tenantIsolationGroupConfigDTO.getMembers())
                          .build())
              .collect(Collectors.toList());

      responseObserver.onNext(
          GetAllTenantIsolationGroupConfigsResponse.newBuilder().addAllConfigs(configs).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("getAllTenantIsolationGroupConfigs failed ", e);
      responseObserver.onError(e);
    }
  }

  public void createTenantIsolationGroupConfig(
      CreateTenantIsolationGroupConfigRequest request,
      StreamObserver<CreateTenantIsolationGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      TenantIsolationGroupConfigDTO upsertedConfigDTO =
          tenantIsolationConfigStore.upsert(
              TenantIsolationGroupConfigDTO.builder()
                  .groupName(request.getConfig().getGroupName())
                  .weight(request.getConfig().getWeight())
                  .members(request.getConfig().getMembersList())
                  .build());
      responseObserver.onNext(
          CreateTenantIsolationGroupConfigResponse.newBuilder()
              .setConfig(
                  TenantIsolationGroupConfig.newBuilder()
                      .setGroupName(upsertedConfigDTO.getGroupName())
                      .setWeight(upsertedConfigDTO.getWeight())
                      .addAllMembers(upsertedConfigDTO.getMembers())
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error(
          String.format(
              "createTenantIsolationGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }

  /** */
  public void updateTenantIsolationGroupConfig(
      UpdateTenantIsolationGroupConfigRequest request,
      StreamObserver<UpdateTenantIsolationGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      TenantIsolationGroupConfigDTO upsertedConfigDTO =
          tenantIsolationConfigStore.upsert(
              TenantIsolationGroupConfigDTO.builder()
                  .groupName(request.getConfig().getGroupName())
                  .weight(request.getConfig().getWeight())
                  .members(request.getConfig().getMembersList())
                  .build());
      responseObserver.onNext(
          UpdateTenantIsolationGroupConfigResponse.newBuilder()
              .setConfig(
                  TenantIsolationGroupConfig.newBuilder()
                      .setGroupName(upsertedConfigDTO.getGroupName())
                      .setWeight(upsertedConfigDTO.getWeight())
                      .addAllMembers(upsertedConfigDTO.getMembers())
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error(
          String.format(
              "updateTenantIsolationGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }

  /** */
  public void deleteTenantIsolationGroupConfig(
      DeleteTenantIsolationGroupConfigRequest request,
      StreamObserver<DeleteTenantIsolationGroupConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      tenantIsolationConfigStore.delete(request.getGroupName());
      responseObserver.onNext(DeleteTenantIsolationGroupConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error(
          String.format(
              "deleteTenantIsolationGroupConfig failed for request {%s} with error ", request),
          e);
      responseObserver.onError(e);
    }
  }
}
