package ai.traceable.data.exfiltration.config.service.detection.rule;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.exfiltration.config.service.v1.CreateDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.CreateDataExfiltrationDetectionRuleResponse;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfig;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfigServiceGrpc.DataExfiltrationDetectionRuleConfigServiceImplBase;
import ai.traceable.data.exfiltration.config.service.v1.DeleteDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.DeleteDataExfiltrationDetectionRuleResponse;
import ai.traceable.data.exfiltration.config.service.v1.GetDataExfiltrationDetectionRulesRequest;
import ai.traceable.data.exfiltration.config.service.v1.GetDataExfiltrationDetectionRulesResponse;
import ai.traceable.data.exfiltration.config.service.v1.UpdateDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.UpdateDataExfiltrationDetectionRuleResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DataExfiltrationDetectionRulesConfigServiceImpl
    extends DataExfiltrationDetectionRuleConfigServiceImplBase {

  private final DataExfiltrationDetectionRulesStore dataExfiltrationDetectionRulesStore;
  private final DataExfiltrationDetectionRulesRequestValidator requestValidator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DataExfiltrationDetectionRulesConfigServiceImpl(
      DataExfiltrationDetectionRulesStore ruleStore,
      DataExfiltrationDetectionRulesRequestValidator requestValidator,
      UuidGenerator uuidGenerator) {
    this.dataExfiltrationDetectionRulesStore = ruleStore;
    this.requestValidator = requestValidator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public void getDataExfiltrationDetectionRules(
      GetDataExfiltrationDetectionRulesRequest request,
      StreamObserver<GetDataExfiltrationDetectionRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateGetRequest(requestContext, request);
      responseObserver.onNext(
          GetDataExfiltrationDetectionRulesResponse.newBuilder()
              .addAllConfigs(
                  dataExfiltrationDetectionRulesStore.getAllConfigData(
                      requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("getDataExfiltrationDetectionRules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createDataExfiltrationDetectionRule(
      CreateDataExfiltrationDetectionRuleRequest request,
      StreamObserver<CreateDataExfiltrationDetectionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateCreateRequest(requestContext, request);
      responseObserver.onNext(
          CreateDataExfiltrationDetectionRuleResponse.newBuilder()
              .setConfig(
                  dataExfiltrationDetectionRulesStore
                      .upsertObject(
                          requestContext,
                          DataExfiltrationDetectionRuleConfig.newBuilder()
                              .setId(uuidGenerator.generateRandomId())
                              .setRuleData(request.getRuleData())
                              .build())
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("createDataExfiltrationDetectionRule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataExfiltrationDetectionRule(
      UpdateDataExfiltrationDetectionRuleRequest request,
      StreamObserver<UpdateDataExfiltrationDetectionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateUpdateRequest(requestContext, request);
      responseObserver.onNext(
          UpdateDataExfiltrationDetectionRuleResponse.newBuilder()
              .setConfig(
                  dataExfiltrationDetectionRulesStore
                      .upsertObject(requestContext, request.getRuleConfig())
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("updateDataExfiltrationDetectionRule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataExfiltrationDetectionRule(
      DeleteDataExfiltrationDetectionRuleRequest request,
      StreamObserver<DeleteDataExfiltrationDetectionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateDeleteRequest(requestContext, request);
      dataExfiltrationDetectionRulesStore
          .deleteObject(requestContext, request.getRuleId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteDataExfiltrationDetectionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("deleteDataExfiltrationInclussionRule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
