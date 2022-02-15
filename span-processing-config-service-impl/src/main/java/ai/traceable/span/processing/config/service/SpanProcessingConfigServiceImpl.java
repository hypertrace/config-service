package ai.traceable.span.processing.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateExcludeSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteExcludeSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.ExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.UpdateExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateExcludeSpanRuleResponse;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {
  private final SpanProcessingConfigRequestValidator validator;
  private final ExcludeSpanRulesConfigStore ruleStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  SpanProcessingConfigServiceImpl(
      ExcludeSpanRulesConfigStore ruleStore,
      SpanProcessingConfigRequestValidator requestValidator,
      UuidGenerator uuidGenerator) {
    this.validator = requestValidator;
    this.ruleStore = ruleStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public void getAllExcludeSpanRules(
      GetAllExcludeSpanRulesRequest request,
      StreamObserver<GetAllExcludeSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllExcludeSpanRulesResponse.newBuilder()
              .addAllRules(ruleStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all exclude span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createExcludeSpanRule(
      CreateExcludeSpanRuleRequest request,
      StreamObserver<CreateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle prorities
      ExcludeSpanRule newRule =
          ExcludeSpanRule.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateExcludeSpanRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, newRule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating exclude span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateExcludeSpanRule(
      UpdateExcludeSpanRuleRequest request,
      StreamObserver<UpdateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateExcludeSpanRule updateExcludeSpanRule = request.getRule();
      ExcludeSpanRule existingRule =
          this.ruleStore
              .getData(requestContext, updateExcludeSpanRule.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      ExcludeSpanRule updatedRule = buildUpdatedRule(existingRule, updateExcludeSpanRule);

      responseObserver.onNext(
          UpdateExcludeSpanRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, updatedRule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteExcludeSpanRule(
      DeleteExcludeSpanRuleRequest request,
      StreamObserver<DeleteExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteExcludeSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private ExcludeSpanRule buildUpdatedRule(
      ExcludeSpanRule existingRule, UpdateExcludeSpanRule updateExcludeSpanRule) {
    return ExcludeSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            ExcludeSpanRuleInfo.newBuilder()
                .setName(updateExcludeSpanRule.getName())
                .setFilter(updateExcludeSpanRule.getFilter())
                .build())
        .build();
  }
}
