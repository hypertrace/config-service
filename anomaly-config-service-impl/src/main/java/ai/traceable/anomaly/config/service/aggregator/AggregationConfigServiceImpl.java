package ai.traceable.anomaly.config.service.aggregator;

import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigResponse;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AggregationConfigServiceImpl
    extends EventAggregationConfigServiceGrpc.EventAggregationConfigServiceImplBase {
  private final AggregationConfigRequestValidator aggregationConfigRequestValidator;
  private final AggregationConfigManager aggregationConfigManager;

  @Inject
  public AggregationConfigServiceImpl(
      AggregationConfigRequestValidator aggregationConfigRequestValidator,
      AggregationConfigManager aggregationConfigManager) {
    this.aggregationConfigRequestValidator = aggregationConfigRequestValidator;
    this.aggregationConfigManager = aggregationConfigManager;
  }

  public void updateScopedAnomalyEventAggregationConfig(
      UpdateScopedAnomalyEventAggregationConfigRequest request,
      StreamObserver<UpdateScopedAnomalyEventAggregationConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      aggregationConfigRequestValidator.validateUpdateAggregatorConfigurationRequest(
          requestContext, request);
      UpdateScopedAnomalyEventAggregationConfigResponse response =
          UpdateScopedAnomalyEventAggregationConfigResponse.newBuilder()
              .setScopedAnomalyEventAggregationConfig(
                  aggregationConfigManager.updateScopedAnomalyEventAggregationConfig(
                      RequestContext.CURRENT.get(),
                      request.getScopedAnomalyEventAggregationConfig()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Scoped AggregationConfig RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void getAllScopedAnomalyEventAggregationConfigs(
      GetAllScopedAnomalyEventAggregationConfigsRequest request,
      StreamObserver<GetAllScopedAnomalyEventAggregationConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      aggregationConfigRequestValidator.validateGetAllScopedAnomalyEventAggregationConfigsRequest(
          requestContext, request);
      GetAllScopedAnomalyEventAggregationConfigsResponse response =
          GetAllScopedAnomalyEventAggregationConfigsResponse.newBuilder()
              .addAllScopedAnomalyEventAggregationConfigs(
                  aggregationConfigManager.getAllScopedAnomalyEventAggregationConfigs(
                      RequestContext.CURRENT.get()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("GetAll Scoped AggregationConfigs RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void deleteScopedAnomalyEventAggregationConfig(
      DeleteScopedAnomalyEventAggregationConfigRequest request,
      StreamObserver<DeleteScopedAnomalyEventAggregationConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      aggregationConfigRequestValidator.validateDeleteScopedAnomalyEventAggregationConfigRequest(
          requestContext, request);
      aggregationConfigManager.deleteScopedAnomalyEventAggregationConfig(
          RequestContext.CURRENT.get(), request.getConfigScope());
      responseObserver.onNext(
          DeleteScopedAnomalyEventAggregationConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Scoped AggregationConfig RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void getAllUnresolvedScopedAnomalyEventAggregationConfigs(
      GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest request,
      StreamObserver<GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse>
          responseObserver) {
    try {
      aggregationConfigRequestValidator
          .validateGetAllUnresolvedScopedAnomalyEventAggregationConfigs(
              RequestContext.CURRENT.get(), request);
      GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse response =
          GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse.newBuilder()
              .addAllScopedAnomalyEventAggregationConfigs(
                  aggregationConfigManager.getAllUnresolvedScopedAnomalyEventAggregationConfigs(
                      RequestContext.CURRENT.get()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "GetAll Unresolved Scoped AggregationConfigs RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void getScopedAnomalyEventAggregationConfig(
      GetScopedAnomalyEventAggregationConfigRequest request,
      StreamObserver<GetScopedAnomalyEventAggregationConfigResponse> responseObserver) {
    try {
      aggregationConfigRequestValidator.validateGetScopedAnomalyEventAggregationConfig(
          RequestContext.CURRENT.get(), request);
      GetScopedAnomalyEventAggregationConfigResponse response =
          GetScopedAnomalyEventAggregationConfigResponse.newBuilder()
              .setScopedAnomalyEventAggregationConfig(
                  aggregationConfigManager.getScopedAnomalyAggregationConfig(
                      RequestContext.CURRENT.get(), request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("GetScoped AggregationConfig RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void getUnresolvedScopedAnomalyEventAggregationConfig(
      GetUnresolvedScopedAnomalyEventAggregationConfigRequest request,
      StreamObserver<GetUnresolvedScopedAnomalyEventAggregationConfigResponse> responseObserver) {
    try {
      aggregationConfigRequestValidator.validateGetUnresolvedScopedAnomalyEventAggregationConfig(
          RequestContext.CURRENT.get(), request);
      GetUnresolvedScopedAnomalyEventAggregationConfigResponse response =
          GetUnresolvedScopedAnomalyEventAggregationConfigResponse.newBuilder()
              .setScopedAnomalyEventAggregationConfig(
                  aggregationConfigManager.getUnresolvedScopedAnomalyEventAggregationConfig(
                      RequestContext.CURRENT.get(), request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("GetUnresolvedScoped AggregationConfig RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
