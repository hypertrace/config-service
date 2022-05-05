package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceImplBase;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingConfigServiceImpl extends RateLimitingConfigServiceImplBase {
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final ActivityEventProducer activityEventProducer;
  private final boolean shouldPublishActivityEvents;

  @Inject
  public RateLimitingConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      ActivityEventProducer activityEventProducer,
      RateLimitingConfigServiceConfig config) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.activityEventProducer = activityEventProducer;
    this.shouldPublishActivityEvents = config.shouldPublishActivityEvents();
  }

  @Override
  public void getRateLimitingRules(
      GetRateLimitingRulesRequest request,
      StreamObserver<GetRateLimitingRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      GetRateLimitingRulesResponse response =
          GetRateLimitingRulesResponse.newBuilder()
              .addAllRules(rulesManager.getRateLimitingRules(context, request.getFilter()))
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
      rulesValidator.validateOrThrow(context, request);
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
      RateLimitingRule deletedRule =
          rulesManager.deleteRateLimitingRule(context, request.getRuleId());
      responseObserver.onNext(DeleteRateLimitingRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();

      if (shouldPublishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            context,
            buildSecurityConfigurationChangeEvent(deletedRule, SecurityConfigurationAction.REMOVE));
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
      rulesValidator.validateOrThrow(context, request);
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

  private SecurityConfigurationChange buildSecurityConfigurationChangeEvent(
      RateLimitingRule rule, SecurityConfigurationAction securityConfigurationAction) {
    return SecurityConfigurationChange.newBuilder()
        .setRuleId(rule.getId())
        .setRuleName(rule.getData().getName())
        .setSecurityConfigurationType(SecurityConfigurationType.RATE_LIMITING_RULE)
        .setSecurityConfigurationAction(securityConfigurationAction)
        .build();
  }
}
