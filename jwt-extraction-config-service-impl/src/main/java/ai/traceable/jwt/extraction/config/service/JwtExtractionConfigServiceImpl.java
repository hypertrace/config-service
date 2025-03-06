package ai.traceable.jwt.extraction.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.jwt.extraction.config.service.converter.JwtExtractionEdgeDecisionConverter;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleResponse;
import ai.traceable.jwt.extraction.config.service.v1.DeleteJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.DeleteJwtExtractionRuleResponse;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionEdgeDecisionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionEdgeDecisionRulesResponse;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesResponse;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceImplBase;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class JwtExtractionConfigServiceImpl extends JwtExtractionConfigServiceImplBase {

  private final JwtExtractionConfigRequestValidator validator;
  private final JwtExtractionRuleManager ruleManager;
  private final JwtExtractionConfigRuleBuilder ruleBuilder;
  private final FeatureCachingClient featureCachingClient;
  private final JwtExtractionEdgeDecisionConverter converter;

  @Override
  public void getJwtExtractionRules(
      GetJwtExtractionRulesRequest request,
      StreamObserver<GetJwtExtractionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetJwtExtractionRulesResponse.newBuilder()
              .addAllRules(this.ruleManager.getAll(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to fetch jwt extraction rules in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateJwtExtractionRule(
      UpdateJwtExtractionRuleRequest request,
      StreamObserver<UpdateJwtExtractionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateJwtExtractionRuleResponse.newBuilder()
              .setRule(this.ruleManager.update(requestContext, this.ruleBuilder.build(request)))
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to update jwt extraction rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void createJwtExtractionRule(
      CreateJwtExtractionRuleRequest request,
      StreamObserver<CreateJwtExtractionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateJwtExtractionRuleResponse.newBuilder()
              .setRule(this.ruleManager.create(requestContext, this.ruleBuilder.build(request)))
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to create jwt extraction rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteJwtExtractionRule(
      DeleteJwtExtractionRuleRequest request,
      StreamObserver<DeleteJwtExtractionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      this.ruleManager.delete(requestContext, request.getId());

      responseObserver.onNext(DeleteJwtExtractionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to delete jwt extraction rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getJwtExtractionEdgeDecisionRules(
      GetJwtExtractionEdgeDecisionRulesRequest request,
      StreamObserver<GetJwtExtractionEdgeDecisionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      validator.validateOrThrow(context, request);

      EdgeDecisionEngineConfig edgeDecisionEngineConfig =
          featureCachingClient.isEdgeDecisionEnabledForTenant(context)
              ? converter.convert(context, ruleManager.getAll(context, request.getFilter()))
              : EdgeDecisionEngineConfig.getDefaultInstance();
      GetJwtExtractionEdgeDecisionRulesResponse response =
          GetJwtExtractionEdgeDecisionRulesResponse.newBuilder()
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
