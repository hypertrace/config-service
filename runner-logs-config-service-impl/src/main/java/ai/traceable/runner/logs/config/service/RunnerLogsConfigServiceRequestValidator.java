package ai.traceable.runner.logs.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.runner.logs.config.service.v1.AstScanLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPurgeConfigRequest;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPurgeConfigRequest;
import com.google.protobuf.Duration;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

class RunnerLogsConfigServiceRequestValidator {

  void validateOrThrow(
      final RequestContext requestContext, final GetRunnerLogsPurgeConfigRequest request) {
    validateRequestContext(requestContext);
  }

  void validateOrThrow(
      final RequestContext requestContext, final GetRunnerLogsPersistenceConfigRequest request) {
    validateRequestContext(requestContext);
  }

  void validateOrThrow(
      final RequestContext requestContext, final UpdateRunnerLogsPurgeConfigRequest request) {
    validateRequestContext(requestContext);
    validateRunnerLogsPurgeConfigUpdate(request.getRunnerLogsPurgeConfigUpdate());
  }

  void validateOrThrow(
      final RequestContext requestContext, final UpdateRunnerLogsPersistenceConfigRequest request) {
    validateRequestContext(requestContext);
    validateRunnerLogsPersistenceConfigUpdate(request.getRunnerLogsPersistenceConfigUpdate());
  }

  void validateRunnerLogsPurgeConfigUpdate(
      final RunnerLogsPurgeConfigUpdate runnerLogsPurgeConfigUpdate) {
    validateAstScanLogsPurgeConfigUpdate(runnerLogsPurgeConfigUpdate.getAstScanLogsPurgeConfig());
  }

  private void validateRunnerLogsPersistenceConfigUpdate(
      final RunnerLogsPersistenceConfigUpdate runnerLogsPersistenceConfigUpdate) {
    validateAstScanLogsPersistenceConfigUpdate(
        runnerLogsPersistenceConfigUpdate.getAstScanLogsPersistenceConfig());
  }

  private void validateAstScanLogsPurgeConfigUpdate(
      final AstScanLogsPurgeConfigUpdate astScanLogsPurgeConfig) {
    if (Duration.getDefaultInstance().equals(astScanLogsPurgeConfig.getRetentionDuration())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Expected retention duration to be set in ast scan logs purge config")
          .asRuntimeException();
    }
  }

  private void validateAstScanLogsPersistenceConfigUpdate(
      final AstScanLogsPersistenceConfigUpdate astScanLogsPersistenceConfig) {
    if (astScanLogsPersistenceConfig.hasMaxPersistLogLines()
        && astScanLogsPersistenceConfig.getMaxPersistLogLines() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Expected non zero max persist log lines to be set in ast scan logs persistence config")
          .asRuntimeException();
    }
  }

  private void validateRequestContext(final RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }
}
