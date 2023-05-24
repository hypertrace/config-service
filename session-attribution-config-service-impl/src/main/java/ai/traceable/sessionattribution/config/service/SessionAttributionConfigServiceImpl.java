package ai.traceable.sessionattribution.config.service;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleGenerator;
import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleStore;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesRequest;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesResponse;
import ai.traceable.sessionattribution.config.service.v1.RankSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.RankSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionConfigServiceGrpc;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.validation.SessionAttributionConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SessionAttributionConfigServiceImpl
    extends SessionAttributionConfigServiceGrpc.SessionAttributionConfigServiceImplBase {
  private final SessionAttributionConfigRequestValidator validator;
  private final SessionAttributionRuleStore ruleStore;
  private final SessionAttributionRuleGenerator ruleGenerator;
  private final RankCalculator<SessionAttributionRule, String> rankCalculator;
  private final ObjectDiffer objectDiffer;

  @Inject
  SessionAttributionConfigServiceImpl(
      SessionAttributionConfigRequestValidator validator,
      SessionAttributionRuleStore ruleStore,
      SessionAttributionRuleGenerator ruleGenerator,
      RankCalculator<SessionAttributionRule, String> rankCalculator,
      ObjectDiffer objectDiffer) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
    this.rankCalculator = rankCalculator;
    this.objectDiffer = objectDiffer;
  }

  @Override
  public void getSessionAttributionRules(
      GetSessionAttributionRulesRequest request,
      StreamObserver<GetSessionAttributionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetRequest(requestContext);
      responseObserver.onNext(
          GetSessionAttributionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error retrieving session attribution rules for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void createSessionAttributionRule(
      CreateSessionAttributionRuleRequest request,
      StreamObserver<CreateSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateCreateRequest(requestContext, request);

      SessionAttributionRule newRule = this.ruleGenerator.generateNewRuleWithoutRank(request);
      List<SessionAttributionRule> existingRules = this.ruleStore.getAllData(requestContext);
      List<SessionAttributionRule> mergedAndRankedRules =
          this.rankCalculator.rankAndMergeNewObject(newRule, existingRules);
      this.ruleStore.upsertObjects(
          requestContext,
          this.objectDiffer.getNewOrUpdatedObjects(existingRules, mergedAndRankedRules));
      SessionAttributionRule createdNewRule =
          mergedAndRankedRules.stream()
              .filter(rule -> rule.getId().equals(newRule.getId()))
              .findFirst()
              .orElseThrow();
      responseObserver.onNext(
          CreateSessionAttributionRuleResponse.newBuilder().setRule(createdNewRule).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error creating session attribution rule {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateSessionAttributionRule(
      UpdateSessionAttributionRuleRequest request,
      StreamObserver<UpdateSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateUpdateRequest(requestContext, request);
      SessionAttributionRule existingRule =
          this.ruleStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      SessionAttributionRule rule =
          this.ruleGenerator.generateRuleFromUpdateRequest(request, existingRule);

      responseObserver.onNext(
          UpdateSessionAttributionRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, rule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error updating session attribution rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteSessionAttributionRule(
      DeleteSessionAttributionRuleRequest request,
      StreamObserver<DeleteSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateDeleteRequest(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      List<SessionAttributionRule> rulesAfterDelete = this.ruleStore.getAllData(requestContext);
      List<SessionAttributionRule> rerankedRules =
          this.rankCalculator.rankFromOrder(rulesAfterDelete);

      List<SessionAttributionRule> newOrUpdatedObjects =
          this.objectDiffer.getNewOrUpdatedObjects(rulesAfterDelete, rerankedRules);
      if (!newOrUpdatedObjects.isEmpty()) {
        this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
      }

      responseObserver.onNext(DeleteSessionAttributionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error deleting session attribution rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void rankSessionAttributionRule(
      RankSessionAttributionRuleRequest request,
      StreamObserver<RankSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateRankRequest(requestContext, request);
      List<SessionAttributionRule> existingRules = this.ruleStore.getAllData(requestContext);
      List<SessionAttributionRule> rerankedRules =
          request.hasPrecedingRuleId()
              ? this.rankCalculator.rerankAfterOtherObject(
                  request.getIdToUpdate(), request.getPrecedingRuleId(), existingRules)
              : this.rankCalculator.rerankAsHighestRank(request.getIdToUpdate(), existingRules);

      List<SessionAttributionRule> newOrUpdatedObjects =
          this.objectDiffer.getNewOrUpdatedObjects(existingRules, rerankedRules);
      if (!newOrUpdatedObjects.isEmpty()) {
        this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
      }
      responseObserver.onNext(RankSessionAttributionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error ranking session attribution rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }
}
