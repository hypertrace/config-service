package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.DetectionExclusionRuleEdgeDecisionConverter;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.RulesMigrationManager;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DetectionExclusionConfigServiceImpl
    extends DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceImplBase {

  private final RulesManager rulesManager;
  private final RulesValidator rulesValidator;
  private final FeatureCachingClient featureCachingClient;
  private final DetectionExclusionRuleEdgeDecisionConverter edgeDecisionConverter;
  private final RulesMigrationManager rulesMigrationManager;

  @Inject
  public DetectionExclusionConfigServiceImpl(
      RulesManager rulesManager,
      RulesValidator rulesValidator,
      FeatureCachingClient featureCachingClient,
      DetectionExclusionRuleEdgeDecisionConverter edgeDecisionConverter,
      RulesMigrationManager rulesMigrationManager) {
    this.rulesManager = rulesManager;
    this.rulesValidator = rulesValidator;
    this.featureCachingClient = featureCachingClient;
    this.edgeDecisionConverter = edgeDecisionConverter;
    this.rulesMigrationManager = rulesMigrationManager;
  }

  @Override
  public void getDetectionExclusionRules(
      GetDetectionExclusionRulesRequest request,
      StreamObserver<GetDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      List<DetectionExclusionRuleRecord> ruleRecords =
          rulesManager.getDetectionExclusionRuleRecords(context, request.getFilter());

      GetDetectionExclusionRulesResponse response =
          GetDetectionExclusionRulesResponse.newBuilder()
              .addAllRules(
                  ruleRecords.stream()
                      .map(DetectionExclusionRuleRecord::getRule)
                      .collect(Collectors.toList()))
              .addAllRuleRecords(ruleRecords)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getDetectionExclusionEdgeDecisionRules(
      GetDetectionExclusionEdgeDecisionRulesRequest request,
      StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      GetDetectionExclusionEdgeDecisionRulesResponse response =
          featureCachingClient.isEdgeDecisionEnabledForTenant(context)
              ? GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                  .setEdgeDecisionEngineConfig(
                      edgeDecisionConverter.convert(
                          context,
                          rulesManager.getDetectionExclusionRules(context, request.getFilter())))
                  .build()
              : GetDetectionExclusionEdgeDecisionRulesResponse.getDefaultInstance();

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
      CreateDetectionExclusionRuleRequest migratedCreateRuleRequest =
          rulesMigrationManager.migrateCreateDetectionExclusionRuleRequest(request);
      List<DetectionExclusionRule> existingRules = getExistingRules(context);
      rulesValidator.validateOrThrow(context, migratedCreateRuleRequest, existingRules);
      DetectionExclusionRule detectionExclusionRule =
          rulesManager.createDetectionExclusionRule(
              context,
              migratedCreateRuleRequest.getRuleScope(),
              migratedCreateRuleRequest.getRuleInfo());
      CreateDetectionExclusionRuleResponse response =
          CreateDetectionExclusionRuleResponse.newBuilder().setRule(detectionExclusionRule).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void bulkUpsertDetectionExclusionRules(
      BulkUpsertDetectionExclusionRulesRequest request,
      StreamObserver<BulkUpsertDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      BulkUpsertDetectionExclusionRulesRequest migratedBulkUpsertRulesRequest =
          rulesMigrationManager.migrateBulkUpsertDetectionExclusionRulesRequest(request);
      rulesValidator.validateOrThrowBulkUpsertRequest(
          context, migratedBulkUpsertRulesRequest.getRulesList());
      List<DetectionExclusionRule> existingRules =
          rulesManager.bulkUpsertDetectionExclusionRule(
              context, migratedBulkUpsertRulesRequest.getRulesList());
      BulkUpsertDetectionExclusionRulesResponse response =
          BulkUpsertDetectionExclusionRulesResponse.newBuilder().addAllRules(existingRules).build();
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
      UpdateDetectionExclusionRuleRequest migratedUpdateRuleRequest =
          rulesMigrationManager.migrateUpdateDetectionExclusionRuleRequest(request);
      List<DetectionExclusionRule> existingRules = getExistingRules(context);
      rulesValidator.validateOrThrow(context, migratedUpdateRuleRequest, existingRules);
      DetectionExclusionRule rule =
          rulesManager.updateDetectionExclusionRule(context, migratedUpdateRuleRequest.getRule());
      UpdateDetectionExclusionRuleResponse response =
          UpdateDetectionExclusionRuleResponse.newBuilder().setRule(rule).build();
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

  @Override
  public void bulkUpdateDetectionExclusionRules(
      BulkUpdateDetectionExclusionRulesRequest request,
      StreamObserver<BulkUpdateDetectionExclusionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      rulesManager.bulkUpdateDetectionExclusionRules(context, request);
      responseObserver.onNext(BulkUpdateDetectionExclusionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to bulk update detection exclusion rules with ids {} :",
          request.getIdsList(),
          exception);
      responseObserver.onError(exception);
    }
  }

  private List<DetectionExclusionRule> getExistingRules(RequestContext context) {
    return rulesManager.getDetectionExclusionRules(context, GetRulesFilter.getDefaultInstance());
  }
}
