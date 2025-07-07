package ai.traceable.ratelimiting.service.v2;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceImplBase;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.converter.RateLimitingEdgeDecisionConverter;
import ai.traceable.ratelimiting.service.v2.rules.migration.RateLimitingMigrationManager;
import ai.traceable.ratelimiting.service.v2.rules.shared.RateLimitingRulesEdgeDecisionFilter;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingConfigServiceImpl extends RateLimitingConfigServiceImplBase {

  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final RateLimitingEdgeDecisionConverter translator;
  private final FeatureCachingClient featureCachingClient;
  private final RateLimitingMigrationManager migrationManager;

  @Inject
  public RateLimitingConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      RateLimitingEdgeDecisionConverter translator,
      FeatureCachingClient featureCachingClient,
      RateLimitingMigrationManager migrationManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.translator = translator;
    this.featureCachingClient = featureCachingClient;
    this.migrationManager = migrationManager;
  }

  @Override
  public void getRateLimitingRules(
      GetRateLimitingRulesRequest request,
      StreamObserver<GetRateLimitingRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      migrationManager.migrateFromChangeLog1IfApplicable(context);
      migrationManager.migrateForRuleEvaluationPointsIfApplicable(context);

      if (request.hasFilter()) { // backward compatibility
        request.toBuilder()
            .setRulesFilter(
                GetRateLimitingRulesFilter.newBuilder()
                    .addAllCategories(request.getFilter().getCategoriesList()));
      }

      GetRateLimitingRulesFilter rulesFilter = request.getRulesFilter();
      List<RateLimitingRule> rateLimitingRules =
          rulesManager.getRateLimitingRules(context, rulesFilter);

      if (rulesFilter.hasFilterEdgeDecisionRules() && rulesFilter.getFilterEdgeDecisionRules()) {
        rateLimitingRules =
            RateLimitingRulesEdgeDecisionFilter.getFilteredRules(
                rateLimitingRules, featureCachingClient.isEdgeDecisionEnabledForTenant(context));
      }

      GetRateLimitingRulesResponse response =
          GetRateLimitingRulesResponse.newBuilder().addAllRules(rateLimitingRules).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateRateLimitingRule(
      UpdateRateLimitingRuleRequest request,
      StreamObserver<UpdateRateLimitingRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      List<RateLimitingRule> existingRules =
          rulesManager.getRateLimitingRules(
              RequestContext.CURRENT.get(), GetRateLimitingRulesFilter.getDefaultInstance());
      UpdateRateLimitingRuleRequest migratedUpdateRuleRequest =
          migrationManager.migrateUpdateRateLimitingRuleRequest(request);
      rulesValidator.validateOrThrow(context, migratedUpdateRuleRequest, existingRules);

      RateLimitingRule rule =
          rulesManager.updateRateLimitingRule(
              context, migratedUpdateRuleRequest.getRuleId(), migratedUpdateRuleRequest.getData());
      UpdateRateLimitingRuleResponse response =
          UpdateRateLimitingRuleResponse.newBuilder().setRule(rule).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteRateLimitingRule(
      DeleteRateLimitingRuleRequest request,
      StreamObserver<DeleteRateLimitingRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      rulesManager.deleteRateLimitingRule(context, request.getRuleId());
      responseObserver.onNext(DeleteRateLimitingRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createRateLimitingRule(
      CreateRateLimitingRuleRequest request,
      StreamObserver<CreateRateLimitingRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      List<RateLimitingRule> existingRules =
          rulesManager.getRateLimitingRules(
              RequestContext.CURRENT.get(), GetRateLimitingRulesFilter.getDefaultInstance());
      CreateRateLimitingRuleRequest migratedCreateRuleRequest =
          migrationManager.migrateCreateRateLimitingRuleRequest(request);
      rulesValidator.validateOrThrow(context, migratedCreateRuleRequest, existingRules);

      RateLimitingRule rule =
          rulesManager.createRateLimitingRule(context, migratedCreateRuleRequest.getData());
      CreateRateLimitingRuleResponse response =
          CreateRateLimitingRuleResponse.newBuilder().setRule(rule).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getRateLimitingRuleModsecRules(
      GetRateLimitingRuleModsecRulesRequest request,
      StreamObserver<GetRateLimitingRuleModsecRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      responseObserver.onNext(
          rulesManager.getRateLimitingModsecRules(context, request.getRulesFilter()));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getRateLimitingEdgeDecisionRules(
      GetRateLimitingEdgeDecisionRulesRequest request,
      StreamObserver<GetRateLimitingEdgeDecisionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      EdgeDecisionEngineConfig edgeDecisionEngineConfig =
          featureCachingClient.isEdgeDecisionEnabledForTenant(context)
              ? translator.convert(
                  context,
                  RateLimitingRulesEdgeDecisionFilter.getConvertibleRules(
                      rulesManager.getRateLimitingRules(context, request.getRulesFilter())))
              : EdgeDecisionEngineConfig.getDefaultInstance();
      GetRateLimitingEdgeDecisionRulesResponse response =
          GetRateLimitingEdgeDecisionRulesResponse.newBuilder()
              .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }
}
