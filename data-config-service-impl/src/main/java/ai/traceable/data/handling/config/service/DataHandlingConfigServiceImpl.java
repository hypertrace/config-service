package ai.traceable.data.handling.config.service;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.data.handling.config.service.store.DataHandlingRuleStore;
import ai.traceable.data.handling.config.service.utils.DataHandlingRuleGenerator;
import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleResponse;
import ai.traceable.data.handling.config.service.v1.DataHandlingConfigServiceGrpc.DataHandlingConfigServiceImplBase;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleResponse;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesRequest;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesResponse;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleResponse;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleResponse;
import ai.traceable.data.handling.config.service.validation.DataHandlingConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DataHandlingConfigServiceImpl extends DataHandlingConfigServiceImplBase {

  private final DataHandlingConfigRequestValidator validator;
  private final DataHandlingRuleStore ruleStore;
  private final ObjectDiffer differ;
  private final DataHandlingRuleRankCalculator rankCalculator;
  private final DataHandlingRuleGenerator ruleGenerator;

  @Inject
  DataHandlingConfigServiceImpl(
      DataHandlingConfigRequestValidator validator,
      DataHandlingRuleStore ruleStore,
      ObjectDiffer differ,
      DataHandlingRuleRankCalculator rankCalculator,
      DataHandlingRuleGenerator ruleGenerator) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.differ = differ;
    this.rankCalculator = rankCalculator;
    this.ruleGenerator = ruleGenerator;
  }

  @Override
  public void getDataHandlingRules(
      GetDataHandlingRulesRequest request,
      StreamObserver<GetDataHandlingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetDataHandlingRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllObjects(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error retrieving data handling rules", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createDataHandlingRule(
      CreateDataHandlingRuleRequest request,
      StreamObserver<CreateDataHandlingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      DataHandlingRule newRule = this.ruleGenerator.generateNewRuleWithoutRank(request);
      List<DataHandlingRule> existingRules = this.ruleStore.getAllObjects(requestContext);
      List<DataHandlingRule> mergedAndRankedRules =
          this.rankCalculator.rankAndMergeNewObject(newRule, existingRules);
      this.ruleStore.upsertObjects(
          requestContext, this.differ.getNewOrUpdatedObjects(existingRules, mergedAndRankedRules));
      responseObserver.onNext(
          CreateDataHandlingRuleResponse.newBuilder().addAllRules(mergedAndRankedRules).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating data handling rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDataHandlingRule(
      UpdateDataHandlingRuleRequest request,
      StreamObserver<UpdateDataHandlingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      DataHandlingRule existingRule =
          this.ruleStore
              .getObject(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asException);

      DataHandlingRule updatedRule = existingRule.toBuilder().setData(request.getData()).build();

      responseObserver.onNext(
          UpdateDataHandlingRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, updatedRule))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating data handling rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void rankDataHandlingRule(
      RankDataHandlingRuleRequest request,
      StreamObserver<RankDataHandlingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      List<DataHandlingRule> existingRules = this.ruleStore.getAllObjects(requestContext);
      List<DataHandlingRule> rerankedRules =
          request.hasPrecedingRuleId()
              ? this.rankCalculator.rerankAfterOtherObject(
                  request.getRuleIdToUpdate(), request.getPrecedingRuleId(), existingRules)
              : this.rankCalculator.rerankAsHighestRank(request.getRuleIdToUpdate(), existingRules);

      this.ruleStore.upsertObjects(
          requestContext, this.differ.getNewOrUpdatedObjects(existingRules, rerankedRules));
      responseObserver.onNext(
          RankDataHandlingRuleResponse.newBuilder().addAllRules(rerankedRules).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error ranking data handling rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteDataHandlingRule(
      DeleteDataHandlingRuleRequest request,
      StreamObserver<DeleteDataHandlingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      List<DataHandlingRule> rulesAfterDelete = this.ruleStore.getAllObjects(requestContext);
      List<DataHandlingRule> rerankedRules = this.rankCalculator.rankFromOrder(rulesAfterDelete);

      this.ruleStore.upsertObjects(
          requestContext, this.differ.getNewOrUpdatedObjects(rulesAfterDelete, rerankedRules));

      responseObserver.onNext(
          DeleteDataHandlingRuleResponse.newBuilder().addAllRules(rerankedRules).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting data handling rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
