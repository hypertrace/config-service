package ai.traceable.userattribution.config.service;

import ai.traceable.userattribution.config.service.store.UserAttributionRuleGenerator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleRankCalculator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleStore;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.validation.UserAttributionConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class UserAttributionConfigServiceImpl extends UserAttributionConfigServiceImplBase {
  private final UserAttributionConfigRequestValidator validator;
  private final UserAttributionRuleStore ruleStore;
  private final UserAttributionRuleGenerator ruleGenerator;
  private final UserAttributionRuleRankCalculator rankCalculator;
  private final UserAttributionRuleDiffer ruleDiffer;

  @Inject
  UserAttributionConfigServiceImpl(
      UserAttributionConfigRequestValidator validator,
      UserAttributionRuleStore ruleStore,
      UserAttributionRuleGenerator ruleGenerator,
      UserAttributionRuleRankCalculator rankCalculator,
      UserAttributionRuleDiffer ruleDiffer) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
    this.rankCalculator = rankCalculator;
    this.ruleDiffer = ruleDiffer;
  }

  @Override
  public void getUserAttributionRules(
      GetUserAttributionRulesRequest request,
      StreamObserver<GetUserAttributionRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetUserAttributionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getRules(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error retrieving user attribution rules", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createUserAttributionRule(
      CreateUserAttributionRuleRequest request,
      StreamObserver<CreateUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UserAttributionRule newRule = this.ruleGenerator.generateNewRuleWithoutRank(request);
      List<UserAttributionRule> existingRules = this.ruleStore.getRules(requestContext);
      List<UserAttributionRule> rulesToUpdate =
          this.ruleDiffer.filterUnchangedRules(
              existingRules, this.rankCalculator.rankAndMergeNewRule(newRule, existingRules));

      List<UserAttributionRule> updatedRules =
          this.ruleStore.upsertAllRules(requestContext, rulesToUpdate);
      // TODO remove once deprecated api removed
      UserAttributionRule createdNewRule =
          updatedRules.stream()
              .filter(rule -> rule.getId().equals(newRule.getId()))
              .findFirst()
              .orElseThrow();
      responseObserver.onNext(
          CreateUserAttributionRuleResponse.newBuilder()
              .setRule(createdNewRule)
              .addAllRules(updatedRules)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating user attribution rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateUserAttributionRule(
      UpdateUserAttributionRuleRequest request,
      StreamObserver<UpdateUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      UserAttributionRule existingRule =
          this.ruleStore
              .getRule(requestContext, request.getRule().getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      this.validator.validateUpdateOrThrow(existingRule, request.getRule());

      responseObserver.onNext(
          UpdateUserAttributionRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertRule(requestContext, request.getRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteUserAttributionRule(
      DeleteUserAttributionRuleRequest request,
      StreamObserver<DeleteUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      this.ruleStore.deleteRule(requestContext, request.getRuleId());
      List<UserAttributionRule> rulesAfterDelete = this.ruleStore.getRules(requestContext);
      List<UserAttributionRule> rerankedRules =
          this.ruleDiffer.filterUnchangedRules(
              rulesAfterDelete, this.rankCalculator.rankFromOrder(rulesAfterDelete));
      responseObserver.onNext(
          DeleteUserAttributionRuleResponse.newBuilder()
              .addAllRules(this.ruleStore.upsertAllRules(requestContext, rerankedRules))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void rankUserAttributionRule(
      RankUserAttributionRuleRequest request,
      StreamObserver<RankUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      List<UserAttributionRule> existingRules = this.ruleStore.getRules(requestContext);
      List<UserAttributionRule> rerankedRules =
          this.ruleDiffer.filterUnchangedRules(
              existingRules, this.rankCalculator.rerankRules(request, existingRules));
      responseObserver.onNext(
          RankUserAttributionRuleResponse.newBuilder()
              .addAllRules(this.ruleStore.upsertAllRules(requestContext, rerankedRules))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error ranking user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
