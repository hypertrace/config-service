package ai.traceable.span.processing.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.store.SpanProcessingRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSpanProcessingRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanProcessingRuleResponse;
import ai.traceable.span.processing.config.service.v1.ExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.GetAllSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSpanProcessingRulesResponse;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRuleInfo;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRulesResponse;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {
  private final SpanProcessingConfigRequestValidator validator;
  private final SpanProcessingRulesConfigStore ruleStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  SpanProcessingConfigServiceImpl(
      SpanProcessingRulesConfigStore ruleStore,
      SpanProcessingConfigRequestValidator requestValidator,
      UuidGenerator uuidGenerator) {
    this.validator = requestValidator;
    this.ruleStore = ruleStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public void getAllSpanProcessingRules(
      GetAllSpanProcessingRulesRequest request,
      StreamObserver<GetAllSpanProcessingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllSpanProcessingRulesResponse.newBuilder()
              .addAllRules(ruleStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get span processing rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createSpanProcessingRule(
      CreateSpanProcessingRuleRequest request,
      StreamObserver<CreateSpanProcessingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle prorities
      SpanProcessingRule newRule =
          SpanProcessingRule.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateSpanProcessingRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, newRule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating span processing rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateSpanProcessingRules(
      UpdateSpanProcessingRulesRequest request,
      StreamObserver<UpdateSpanProcessingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      List<SpanProcessingRule> updatedRules = new ArrayList<>();
      for (UpdateSpanProcessingRule rule : request.getRulesList()) {
        SpanProcessingRule existingRule =
            this.ruleStore
                .getData(requestContext, rule.getId())
                .orElseThrow(Status.NOT_FOUND::asException);
        SpanProcessingRule updatedRule = buildUpdatedRule(existingRule, rule);
        updatedRules.add(this.ruleStore.upsertObject(requestContext, updatedRule).getData());
      }

      responseObserver.onNext(
          UpdateSpanProcessingRulesResponse.newBuilder().addAllRules(updatedRules).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating span processing rules: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteSpanProcessingRule(
      DeleteSpanProcessingRuleRequest request,
      StreamObserver<DeleteSpanProcessingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSpanProcessingRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting span processing rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private SpanProcessingRule buildUpdatedRule(
      SpanProcessingRule existingRule, UpdateSpanProcessingRule updateSpanProcessingRule) {
    SpanProcessingRuleInfo.Builder spanProcessingRuleInfoBuilder =
        SpanProcessingRuleInfo.newBuilder();
    if (updateSpanProcessingRule.hasExcludeSpanRule()) {
      spanProcessingRuleInfoBuilder
          .setName(updateSpanProcessingRule.getName())
          .setExcludeSpanRule(
              ExcludeSpanRule.newBuilder()
                  .setFilter(updateSpanProcessingRule.getExcludeSpanRule().getFilter())
                  .build());
    }
    return SpanProcessingRule.newBuilder(existingRule)
        .setRuleInfo(spanProcessingRuleInfoBuilder)
        .build();
  }
}
