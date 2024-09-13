package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DetectionExclusionConfigServiceImpl
    extends DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceImplBase {

  private final RulesManager rulesManager;
  private final RulesValidator rulesValidator;

  @Inject
  public DetectionExclusionConfigServiceImpl(
      RulesManager rulesManager, RulesValidator rulesValidator) {
    this.rulesManager = rulesManager;
    this.rulesValidator = rulesValidator;
  }

  @Override
  public void getDetectionExclusionRules(
      GetDetectionExclusionRulesRequest request,
      StreamObserver<GetDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      GetDetectionExclusionRulesResponse response =
          GetDetectionExclusionRulesResponse.newBuilder()
              .addAllRules(rulesManager.getDetectionExclusionRules(context, request.getFilter()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createDetectionExclusionRule(
      CreateDetectionExclusionRuleRequest request,
      StreamObserver<CreateDetectionExclusionRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      List<DetectionExclusionRule> existingRules = getExistingRules(context);
      rulesValidator.validateOrThrow(context, request, existingRules);

      CreateDetectionExclusionRuleResponse response =
          CreateDetectionExclusionRuleResponse.newBuilder()
              .setRule(
                  rulesManager.createDetectionExclusionRule(
                      context, request.getRuleScope(), request.getRuleInfo()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void bulkCreateDetectionExclusionRules(
      BulkCreateDetectionExclusionRulesRequest request,
      StreamObserver<BulkCreateDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      List<DetectionExclusionRule> existingRules = getExistingRules(context);
      List<CreateDetectionExclusionRuleData> distinctExclusionRuleCreateRequestList =
          request.getRuleDataList().stream()
              .filter(rule -> isSameRuleNameNotPresent(rule.getRuleInfo().getName(), existingRules))
              .collect(Collectors.toUnmodifiableList());
      rulesValidator.validateOrThrow(context, distinctExclusionRuleCreateRequestList);

      BulkCreateDetectionExclusionRulesResponse response =
          BulkCreateDetectionExclusionRulesResponse.newBuilder()
              .addAllRules(
                  rulesManager.bulkCreateDetectionExclusionRule(
                      context, distinctExclusionRuleCreateRequestList))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDetectionExclusionRule(
      UpdateDetectionExclusionRuleRequest request,
      StreamObserver<UpdateDetectionExclusionRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      List<DetectionExclusionRule> existingRules = getExistingRules(context);
      rulesValidator.validateOrThrow(context, request, existingRules);

      UpdateDetectionExclusionRuleResponse response =
          UpdateDetectionExclusionRuleResponse.newBuilder()
              .setRule(rulesManager.updateDetectionExclusionRule(context, request.getRule()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteDetectionExclusionRule(
      DeleteDetectionExclusionRuleRequest request,
      StreamObserver<DeleteDetectionExclusionRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      rulesManager.deleteDetectionExclusionRule(context, request.getId());

      responseObserver.onNext(DeleteDetectionExclusionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void bulkDeleteDetectionExclusionRules(
      BulkDeleteDetectionExclusionRulesRequest request,
      StreamObserver<BulkDeleteDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      rulesManager.bulkDeleteDetectionExclusionRules(context, request.getIdsList());

      responseObserver.onNext(BulkDeleteDetectionExclusionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getExclusionModsecRules(
      GetExclusionModsecRulesRequest request,
      StreamObserver<GetExclusionModsecRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      responseObserver.onNext(rulesManager.getDetectionExclusionModsecRules(context, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  private List<DetectionExclusionRule> getExistingRules(RequestContext context) {
    return rulesManager.getDetectionExclusionRules(context, GetRulesFilter.getDefaultInstance());
  }

  private boolean isSameRuleNameNotPresent(
      String ruleName, List<DetectionExclusionRule> detectionExclusionRules) {
    return detectionExclusionRules.stream()
        .noneMatch(rule -> rule.getRuleInfo().getName().equals(ruleName));
  }
}
