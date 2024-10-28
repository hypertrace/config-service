package ai.traceable.userattribution.config.service.v2;

import static ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.UserAttributionRuleSource.USER_ATTRIBUTION_RULE_SOURCE_V2;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase;
import ai.traceable.userattribution.config.service.v2.migration.LegacyUserAttributionRuleTranslatingDao;
import ai.traceable.userattribution.config.service.v2.store.UserAttributionV2RuleGenerator;
import ai.traceable.userattribution.config.service.v2.store.UserAttributionV2RuleStore;
import ai.traceable.userattribution.config.service.v2.validation.UserAttributionV2ConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class UserAttributionV2ConfigServiceImpl extends UserAttributionConfigServiceImplBase {
  private final FeatureCachingClient featureCachingClient;
  private final UserAttributionV2ConfigRequestValidator validator;
  private final UserAttributionV2RuleStore ruleStore;
  private final UserAttributionV2RuleGenerator ruleGenerator;
  private final RankCalculator<UserAttributionRule, String> rankCalculator;
  private final ObjectDiffer objectDiffer;
  private final LegacyUserAttributionRuleTranslatingDao legacyRuleStore;

  @Inject
  UserAttributionV2ConfigServiceImpl(
      FeatureCachingClient featureCachingClient,
      UserAttributionV2ConfigRequestValidator validator,
      UserAttributionV2RuleStore ruleStore,
      UserAttributionV2RuleGenerator ruleGenerator,
      RankCalculator<UserAttributionRule, String> rankCalculator,
      ObjectDiffer objectDiffer,
      LegacyUserAttributionRuleTranslatingDao legacyRuleStore) {
    this.featureCachingClient = featureCachingClient;
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
    this.rankCalculator = rankCalculator;
    this.objectDiffer = objectDiffer;
    this.legacyRuleStore = legacyRuleStore;
  }

  @Override
  public void getUserAttributionRules(
      GetUserAttributionRulesRequest request,
      StreamObserver<GetUserAttributionRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      List<UserAttributionRule> allRules =
          this.ruleStore.getAllConfigData(requestContext, request.getFilter());
      // fallback to legacy rules if no rules are found in new store
      // and source is not set to only v2 rules
      if (allRules.isEmpty() && !request.getRuleSource().equals(USER_ATTRIBUTION_RULE_SOURCE_V2)) {
        allRules =
            this.legacyRuleStore.getUserAttributionRulesFromLegacyStore(
                requestContext, request.getFilter());
      }
      responseObserver.onNext(
          GetUserAttributionRulesResponse.newBuilder().addAllRules(allRules).build());
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
      migrateAndDeleteLegacyRules(requestContext);
      UserAttributionRule newRule = this.ruleGenerator.generateNewRuleWithoutRank(request);
      List<UserAttributionRule> existingRules = this.ruleStore.getAllConfigData(requestContext);
      this.validator.validateOrThrow(existingRules, newRule);
      List<UserAttributionRule> mergedAndRankedRules =
          this.rankCalculator.rankAndMergeNewObject(newRule, existingRules);
      this.ruleStore.upsertObjects(
          requestContext,
          this.objectDiffer.getNewOrUpdatedObjects(existingRules, mergedAndRankedRules));
      responseObserver.onNext(
          CreateUserAttributionRuleResponse.newBuilder().addAllRules(mergedAndRankedRules).build());
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
      migrateAndDeleteLegacyRules(requestContext);
      String ruleId = request.getId();
      UserAttributionRule existingRule =
          this.ruleStore.getData(requestContext, ruleId).orElseThrow(Status.NOT_FOUND::asException);
      UserAttributionRule updatedRule =
          this.ruleGenerator.generateUpdatedRule(request, existingRule);
      this.validator.validateOrThrow(existingRule, updatedRule);
      this.validator.validateOrThrow(this.ruleStore.getAllConfigData(requestContext), updatedRule);

      responseObserver.onNext(
          UpdateUserAttributionRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, updatedRule).getData())
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
      migrateAndDeleteLegacyRules(requestContext);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      List<UserAttributionRule> rulesAfterDelete = this.ruleStore.getAllConfigData(requestContext);
      List<UserAttributionRule> rerankedRules = this.rankCalculator.rankFromOrder(rulesAfterDelete);

      List<UserAttributionRule> newOrUpdatedObjects =
          this.objectDiffer.getNewOrUpdatedObjects(rulesAfterDelete, rerankedRules);
      if (!newOrUpdatedObjects.isEmpty()) {
        this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
      }

      responseObserver.onNext(DeleteUserAttributionRuleResponse.getDefaultInstance());
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
      migrateAndDeleteLegacyRules(requestContext);
      List<UserAttributionRule> existingRules = this.ruleStore.getAllConfigData(requestContext);
      List<UserAttributionRule> rerankedRules =
          request.hasPrecedingRuleId()
              ? this.rankCalculator.rerankAfterOtherObject(
                  request.getIdToUpdate(), request.getPrecedingRuleId(), existingRules)
              : this.rankCalculator.rerankAsHighestRank(request.getIdToUpdate(), existingRules);

      List<UserAttributionRule> newOrUpdatedObjects =
          this.objectDiffer.getNewOrUpdatedObjects(existingRules, rerankedRules);
      if (!newOrUpdatedObjects.isEmpty()) {
        this.ruleStore.upsertObjects(requestContext, newOrUpdatedObjects);
      }
      responseObserver.onNext(
          RankUserAttributionRuleResponse.newBuilder().addAllRules(rerankedRules).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error ranking user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private void migrateAndDeleteLegacyRules(RequestContext requestContext) {
    // don't perform migration of feature flag is not enabled
    if (!featureCachingClient.isUserAttributionV3Enabled(requestContext)) {
      return;
    }
    List<UserAttributionRule> allLegacyRules =
        this.legacyRuleStore.getAllUserAttributionRulesFromLegacyStore(requestContext);
    if (!allLegacyRules.isEmpty()) {
      this.ruleStore.upsertObjects(requestContext, allLegacyRules);
      List<String> allLegacyRuleIds =
          allLegacyRules.stream()
              .map(UserAttributionRule::getId)
              .collect(Collectors.toUnmodifiableList());
      legacyRuleStore.deleteMultipleUserAttributionRulesFromLegacyStore(
          requestContext, allLegacyRuleIds);
    }
  }
}
