package ai.traceable.data.parsing.config.service.v1;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc.DataParsingConfigServiceImplBase;
import ai.traceable.data.parsing.config.service.v1.store.DataParsingRuleStore;
import ai.traceable.data.parsing.config.service.v1.utils.DataParsingConfigGenerator;
import ai.traceable.data.parsing.config.service.v1.utils.DataParsingConfigRankCalculator;
import ai.traceable.data.parsing.config.service.v1.validation.DataParsingConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * In-memory implementation for DataParsingConfigService API. Replace with DB-backed logic as
 * needed.
 */
@Slf4j
public class DataParsingConfigServiceImpl extends DataParsingConfigServiceImplBase {

  private final DataParsingConfigRequestValidator validator;
  private final DataParsingRuleStore ruleStore;
  private final DataParsingConfigGenerator generator;
  private final DataParsingConfigRankCalculator rankCalculator;
  private final ObjectDiffer differ;

  @Inject
  public DataParsingConfigServiceImpl(
      DataParsingConfigRequestValidator validator,
      DataParsingRuleStore ruleStore,
      DataParsingConfigGenerator generator,
      DataParsingConfigRankCalculator rankCalculator,
      ObjectDiffer differ) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.generator = generator;
    this.rankCalculator = rankCalculator;
    this.differ = differ;
  }

  @Override
  public void getDataParsingRules(
      GetDataParsingRulesRequest request,
      StreamObserver<GetDataParsingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      List<DataParsingConfig> configs =
          this.ruleStore.getDataParsingConfigs(requestContext, request.getFilter());
      responseObserver.onNext(
          GetDataParsingRulesResponse.newBuilder().addAllDataParsingConfigs(configs).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error retrieving data parsing configs", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createDataParsingRule(
      CreateDataParsingRuleRequest request,
      StreamObserver<CreateDataParsingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      DataParsingConfig newConfig = generator.generateNewConfig(request);
      List<DataParsingConfig> existingConfigs = this.ruleStore.getAllData(requestContext);
      List<DataParsingConfig> mergedAndRankedConfigs =
          this.rankCalculator.rankAndMergeNewObject(newConfig, existingConfigs);
      this.ruleStore.upsertObjects(requestContext, mergedAndRankedConfigs);
      responseObserver.onNext(
          CreateDataParsingRuleResponse.newBuilder().setDataParsingConfig(newConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating data parsing config", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDataParsingRule(
      UpdateDataParsingRuleRequest request,
      StreamObserver<UpdateDataParsingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      DataParsingConfig existingCpnfig =
          this.ruleStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asException);

      DataParsingConfig updatedConfig =
          existingCpnfig.toBuilder().setDataParsingRule(request.getDataParsingRule()).build();
      this.ruleStore.upsertObject(requestContext, updatedConfig);
      responseObserver.onNext(
          UpdateDataParsingRuleResponse.newBuilder().setDataParsingConfig(updatedConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating data parsing config", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteDataParsingRule(
      DeleteDataParsingRuleRequest request,
      StreamObserver<DeleteDataParsingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      this.ruleStore.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteDataParsingRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting data parsing config", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void rankDataParsingConfig(
      RankDataParsingConfigRequest request,
      StreamObserver<RankDataParsingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      // No validation for now, add if needed
      List<DataParsingConfig> existingConfigs = this.ruleStore.getAllData(requestContext);
      List<DataParsingConfig> rerankedConfigs =
          request.hasPrecedingConfigId()
              ? this.rankCalculator.rerankAfterOtherObject(
                  request.getConfigIdToUpdate(), request.getPrecedingConfigId(), existingConfigs)
              : this.rankCalculator.rerankAsHighestRank(
                  request.getConfigIdToUpdate(), existingConfigs);
      List<DataParsingConfig> newOrUpdatedConfigs =
          this.differ.getNewOrUpdatedObjects(existingConfigs, rerankedConfigs);
      if (!newOrUpdatedConfigs.isEmpty()) {
        this.ruleStore.upsertObjects(requestContext, newOrUpdatedConfigs);
      }
      responseObserver.onNext(
          RankDataParsingConfigResponse.newBuilder()
              .addAllDataParsingConfigs(rerankedConfigs)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error ranking data parsing config: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
