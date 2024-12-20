package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.ratelimiting.config.service.v2.Category;
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
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingConfigServiceImpl extends RateLimitingConfigServiceImplBase {
  private static final Map<Category, SecurityConfigurationType>
      CATEGORY_TO_SECURITY_CONFIGURATION_TYPE_MAP =
          Map.of(
              Category.CATEGORY_RATE_LIMITING, SecurityConfigurationType.RATE_LIMITING_RULE,
              Category.CATEGORY_ENUMERATION, SecurityConfigurationType.ENUMERATION_RULE,
              Category.CATEGORY_DATA_EXFILTRATION,
                  SecurityConfigurationType.DATA_LOSS_PREVENTION_RULE);
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final ActivityEventProducer activityEventProducer;
  private final boolean shouldPublishActivityEvents;
  private final RateLimitingEdgeDecisionConverter translator;

  @Inject
  public RateLimitingConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      ActivityEventProducer activityEventProducer,
      RateLimitingConfigServiceConfig config,
      RateLimitingEdgeDecisionConverter translator) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.activityEventProducer = activityEventProducer;
    this.shouldPublishActivityEvents = config.shouldPublishActivityEvents();
    this.translator = translator;
  }

  @Override
  public void getRateLimitingRules(
      GetRateLimitingRulesRequest request,
      StreamObserver<GetRateLimitingRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);

      if (request.hasFilter()) { // backward compatibility
        request.toBuilder()
            .setRulesFilter(
                GetRateLimitingRulesFilter.newBuilder()
                    .addAllCategories(request.getFilter().getCategoriesList()));
      }

      GetRateLimitingRulesResponse response =
          GetRateLimitingRulesResponse.newBuilder()
              .addAllRules(rulesManager.getRateLimitingRules(context, request.getRulesFilter()))
              .build();

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
      rulesValidator.validateOrThrow(context, request, existingRules);
      UpdateRateLimitingRuleResponse response =
          UpdateRateLimitingRuleResponse.newBuilder()
              .setRule(
                  rulesManager.updateRateLimitingRule(
                      context, request.getRuleId(), request.getData()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();

      if (shouldPublishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            context,
            buildSecurityConfigurationChangeEvent(
                response.getRule(), SecurityConfigurationAction.UPDATE));
      }
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
      Optional<RateLimitingRule> deletedRule =
          rulesManager.deleteRateLimitingRule(context, request.getRuleId());
      responseObserver.onNext(DeleteRateLimitingRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();

      if (shouldPublishActivityEvents && deletedRule.isPresent()) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            context,
            buildSecurityConfigurationChangeEvent(
                deletedRule.get(), SecurityConfigurationAction.REMOVE));
      }
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
      rulesValidator.validateOrThrow(context, request, existingRules);
      CreateRateLimitingRuleResponse response =
          CreateRateLimitingRuleResponse.newBuilder()
              .setRule(rulesManager.createRateLimitingRule(context, request.getData()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();

      if (shouldPublishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            context,
            buildSecurityConfigurationChangeEvent(
                response.getRule(), SecurityConfigurationAction.ADD));
      }
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

      GetRateLimitingEdgeDecisionRulesResponse response =
          GetRateLimitingEdgeDecisionRulesResponse.newBuilder()
              .setEdgeDecisionEngineConfig(
                  translator.convert(
                      context,
                      rulesManager.getRateLimitingRules(context, request.getRulesFilter())))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  private SecurityConfigurationChange buildSecurityConfigurationChangeEvent(
      RateLimitingRule rule, SecurityConfigurationAction securityConfigurationAction) {
    return SecurityConfigurationChange.newBuilder()
        .setRuleId(rule.getId())
        .setRuleName(rule.getData().getName())
        .setSecurityConfigurationType(
            CATEGORY_TO_SECURITY_CONFIGURATION_TYPE_MAP.get(rule.getData().getCategory()))
        .setSecurityConfigurationAction(securityConfigurationAction)
        .build();
  }
}
