package ai.traceable.anomaly.config.service.aggregator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigResponse;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

public class AggregationConfigServiceImplTest {
  private final AggregationConfigRequestValidator validator =
      mock(AggregationConfigRequestValidator.class);
  private final AggregationConfigManager configManager = mock(AggregationConfigManager.class);
  private final AggregationConfigServiceImpl aggregationConfigService =
      new AggregationConfigServiceImpl(validator, configManager);

  @Test
  void testGetScopedAggregationConfig() {
    GetScopedAnomalyEventAggregationConfigRequest request =
        GetScopedAnomalyEventAggregationConfigRequest.newBuilder().build();
    StreamObserver<GetScopedAnomalyEventAggregationConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(validator)
        .validateGetScopedAnomalyEventAggregationConfig(RequestContext.CURRENT.get(), request);
    aggregationConfigService.getScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doNothing()
        .when(validator)
        .validateGetScopedAnomalyEventAggregationConfig(RequestContext.CURRENT.get(), request);
    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .getScopedAnomalyAggregationConfig(any(), any());
    aggregationConfigService.getScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    GetScopedAnomalyEventAggregationConfigResponse response =
        GetScopedAnomalyEventAggregationConfigResponse.newBuilder()
            .setScopedAnomalyEventAggregationConfig(
                ScopedAnomalyEventAggregationConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyEventAggregationConfig())
        .when(configManager)
        .getScopedAnomalyAggregationConfig(any(), any());
    aggregationConfigService.getScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllScopedAggregationConfig() {
    GetAllScopedAnomalyEventAggregationConfigsRequest request =
        GetAllScopedAnomalyEventAggregationConfigsRequest.newBuilder().build();
    StreamObserver<GetAllScopedAnomalyEventAggregationConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .getAllScopedAnomalyEventAggregationConfigs(any());
    aggregationConfigService.getAllScopedAnomalyEventAggregationConfigs(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    GetAllScopedAnomalyEventAggregationConfigsResponse response =
        GetAllScopedAnomalyEventAggregationConfigsResponse.newBuilder()
            .addAllScopedAnomalyEventAggregationConfigs(
                List.of(ScopedAnomalyEventAggregationConfig.getDefaultInstance()))
            .build();
    doNothing()
        .when(validator)
        .validateGetAllScopedAnomalyEventAggregationConfigsRequest(
            RequestContext.CURRENT.get(), request);
    doReturn(response.getScopedAnomalyEventAggregationConfigsList())
        .when(configManager)
        .getAllScopedAnomalyEventAggregationConfigs(any());
    aggregationConfigService.getAllScopedAnomalyEventAggregationConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateScopedAggregationConfig() {
    UpdateScopedAnomalyEventAggregationConfigRequest request =
        UpdateScopedAnomalyEventAggregationConfigRequest.newBuilder().build();
    StreamObserver<UpdateScopedAnomalyEventAggregationConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(validator)
        .validateUpdateAggregatorConfigurationRequest(RequestContext.CURRENT.get(), request);
    aggregationConfigService.updateScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doNothing()
        .when(validator)
        .validateUpdateAggregatorConfigurationRequest(RequestContext.CURRENT.get(), request);
    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .updateScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.updateScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    UpdateScopedAnomalyEventAggregationConfigResponse response =
        UpdateScopedAnomalyEventAggregationConfigResponse.newBuilder()
            .setScopedAnomalyEventAggregationConfig(
                ScopedAnomalyEventAggregationConfig.getDefaultInstance())
            .build();
    doReturn(response.getScopedAnomalyEventAggregationConfig())
        .when(configManager)
        .updateScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.updateScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetUnresolvedScopedAggregationConfig() {
    GetUnresolvedScopedAnomalyEventAggregationConfigRequest request =
        GetUnresolvedScopedAnomalyEventAggregationConfigRequest.newBuilder().build();
    StreamObserver<GetUnresolvedScopedAnomalyEventAggregationConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(validator)
        .validateGetUnresolvedScopedAnomalyEventAggregationConfig(
            RequestContext.CURRENT.get(), request);
    aggregationConfigService.getUnresolvedScopedAnomalyEventAggregationConfig(
        request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doNothing()
        .when(validator)
        .validateGetUnresolvedScopedAnomalyEventAggregationConfig(
            RequestContext.CURRENT.get(), request);
    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .getUnresolvedScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.getUnresolvedScopedAnomalyEventAggregationConfig(
        request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    GetUnresolvedScopedAnomalyEventAggregationConfigResponse response =
        GetUnresolvedScopedAnomalyEventAggregationConfigResponse.newBuilder()
            .setScopedAnomalyEventAggregationConfig(
                ScopedAnomalyEventAggregationConfig.getDefaultInstance())
            .build();
    doReturn(response.getScopedAnomalyEventAggregationConfig())
        .when(configManager)
        .getUnresolvedScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.getUnresolvedScopedAnomalyEventAggregationConfig(
        request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllUnresolvedScopedAggregationConfig() {
    GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest request =
        GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest.newBuilder().build();
    StreamObserver<GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .getAllUnresolvedScopedAnomalyEventAggregationConfigs(any());
    aggregationConfigService.getAllUnresolvedScopedAnomalyEventAggregationConfigs(
        request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse response =
        GetAllUnresolvedScopedAnomalyEventAggregationConfigsResponse.newBuilder()
            .addScopedAnomalyEventAggregationConfigs(
                ScopedAnomalyEventAggregationConfig.getDefaultInstance())
            .build();
    doReturn(response.getScopedAnomalyEventAggregationConfigsList())
        .when(configManager)
        .getAllUnresolvedScopedAnomalyEventAggregationConfigs(any());
    aggregationConfigService.getAllUnresolvedScopedAnomalyEventAggregationConfigs(
        request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteScopedAggregationConfig() {
    DeleteScopedAnomalyEventAggregationConfigRequest request =
        DeleteScopedAnomalyEventAggregationConfigRequest.newBuilder().build();
    StreamObserver<DeleteScopedAnomalyEventAggregationConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(validator)
        .validateDeleteScopedAnomalyEventAggregationConfigRequest(
            RequestContext.CURRENT.get(), request);
    aggregationConfigService.deleteScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doNothing()
        .when(validator)
        .validateDeleteScopedAnomalyEventAggregationConfigRequest(
            RequestContext.CURRENT.get(), request);
    doThrow(Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException())
        .when(configManager)
        .deleteScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.deleteScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> "msg".equals(Status.fromThrowable(err).getDescription())));

    ScopedAnomalyEventAggregationConfig deletedScopedAnomalyAggregationConfig =
        ScopedAnomalyEventAggregationConfig.getDefaultInstance();
    DeleteScopedAnomalyEventAggregationConfigResponse response =
        DeleteScopedAnomalyEventAggregationConfigResponse.getDefaultInstance();
    doNothing().when(configManager).deleteScopedAnomalyEventAggregationConfig(any(), any());
    aggregationConfigService.deleteScopedAnomalyEventAggregationConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
