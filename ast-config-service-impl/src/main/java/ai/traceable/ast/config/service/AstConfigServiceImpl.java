package ai.traceable.ast.config.service;

import ai.traceable.ast.config.service.rules.RulesManager;
import ai.traceable.ast.config.service.rules.RulesValidator;
import ai.traceable.ast.config.service.v1.AstConfigServiceGrpc.AstConfigServiceImplBase;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AstConfigServiceImpl extends AstConfigServiceImplBase {
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final AstConfigServiceConfig config;

  @Override
  public void updateScanPurgeConfig(
      UpdateScanPurgeConfigRequest request,
      StreamObserver<UpdateScanPurgeConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      rulesValidator.validateOrThrow(requestContext, request);
      ScanPurgeConfig scanPurgeConfig = rulesManager.updateScanPurgeConfig(requestContext, request);

      responseObserver.onNext(
          UpdateScanPurgeConfigResponse.newBuilder().setPurgeConfig(scanPurgeConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to update scan purge config for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getScanPurgeConfig(
      GetScanPurgeConfigRequest request,
      StreamObserver<GetScanPurgeConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      rulesValidator.validateOrThrow(requestContext, request);
      ScanPurgeConfig scanPurgeConfig =
          rulesManager
              .getScanPurgeConfig(requestContext)
              .orElseGet(this::getDefaultScanPurgeConfig);

      responseObserver.onNext(
          GetScanPurgeConfigResponse.newBuilder().setPurgeConfig(scanPurgeConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to fetch scan purge config with context {}", requestContext, exception);
      responseObserver.onError(exception);
    }
  }

  private ScanPurgeConfig getDefaultScanPurgeConfig() {
    return ScanPurgeConfig.newBuilder().setPurgeDuration(config.getDefaultPurgeDuration()).build();
  }
}
