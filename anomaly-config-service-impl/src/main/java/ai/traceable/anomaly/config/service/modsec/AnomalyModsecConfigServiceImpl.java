package ai.traceable.anomaly.config.service.modsec;

import ai.traceable.anomaly.config.service.modsec.protection.engine.WebAppEvaluationConfigContextManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecValidator;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceImplBase;
import ai.traceable.anomaly.config.service.v1.modsec.GetDefaultModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetDefaultModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyModsecConfigServiceImpl extends AnomalyModsecConfigServiceImplBase {
  private final ModsecValidator validator;
  private final ModsecManager manager;
  private final WebAppEvaluationConfigContextManager webAppEvaluationConfigContextManager;
  private final ModsecRuleVersion defaultModsecRuleVersion;

  @Inject
  public AnomalyModsecConfigServiceImpl(
      ModsecValidator validator,
      ModsecManager manager,
      WebAppEvaluationConfigContextManager webAppEvaluationConfigContextManager,
      ModsecRuleVersion defaultModsecRuleVersion) {
    this.validator = validator;
    this.manager = manager;
    this.webAppEvaluationConfigContextManager = webAppEvaluationConfigContextManager;
    this.defaultModsecRuleVersion = defaultModsecRuleVersion;
  }

  @Override
  public void getModsecCrsRules(
      GetModsecCrsRulesRequest request,
      StreamObserver<GetModsecCrsRulesResponse> responseObserver) {
    Status status = validator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Anomaly Modsec Config Service Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      ModsecManager.ModsecCrsRules crsRules =
          manager.getModsecCrsRules(
              RequestContext.CURRENT.get(),
              (request.getRuleVersion() == ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
                  ? defaultModsecRuleVersion
                  : request.getRuleVersion(),
              request.getTarget(),
              request.getSubRuleTypesList(),
              request.getRemoveDisabledRules(),
              request.getConfigScope());
      GetModsecCrsRulesResponse response =
          GetModsecCrsRulesResponse.newBuilder()
              .addAllModsecCrsRules(crsRules.getModsecCrsRulesData())
              .setAggregatedModsecCrsRulesBlob(crsRules.getAggregatedModsecBlob())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDefaultModsecCrsRules(
      GetDefaultModsecCrsRulesRequest request,
      StreamObserver<GetDefaultModsecCrsRulesResponse> responseObserver) {
    Status status = validator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Anomaly Default Modsec Config Service Request is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      ModsecManager.ModsecCrsRules crsRules =
          manager.getModsecCrsRules(
              request.getSubRuleTypesList(),
              (request.getRuleVersion() == ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
                  ? defaultModsecRuleVersion
                  : request.getRuleVersion(),
              request.getUseTestModsecRules(),
              request.getVersion(),
              true);
      GetDefaultModsecCrsRulesResponse response =
          GetDefaultModsecCrsRulesResponse.newBuilder()
              .addAllModsecCrsRules(crsRules.getModsecCrsRulesData())
              .setAggregatedModsecCrsRulesBlob(crsRules.getAggregatedModsecBlob())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getWebAppEvaluationConfigContext(
      GetWebAppEvaluationConfigContextRequest request,
      StreamObserver<GetWebAppEvaluationConfigContextResponse> responseObserver) {
    try {
      validator.validate(request);
      WebAppEvaluationConfigContext configContext =
          webAppEvaluationConfigContextManager.getWebAppEvaluationConfigContext(
              RequestContext.CURRENT.get(), request);
      GetWebAppEvaluationConfigContextResponse response =
          GetWebAppEvaluationConfigContextResponse.newBuilder()
              .setWebAppEvaluationConfigContext(configContext.toByteString())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
