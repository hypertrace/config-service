package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toPiiFilterConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toValue;
import static ai.traceable.sensitivedata.config.service.TestUtils.ENDPOINT1;
import static ai.traceable.sensitivedata.config.service.TestUtils.ENDPOINT2;
import static ai.traceable.sensitivedata.config.service.TestUtils.TENANT_ID;
import static ai.traceable.sensitivedata.config.service.TestUtils.getParameter;
import static ai.traceable.sensitivedata.config.service.TestUtils.getParameterWithSensitivity;
import static ai.traceable.sensitivedata.config.service.TestUtils.getParametersWithSensitivityList;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.sensitivedata.config.service.v1.Filter;
import ai.traceable.sensitivedata.config.service.v1.GetParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.GetParametersResponse;
import ai.traceable.sensitivedata.config.service.v1.MarkParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithSensitivity;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SensitiveDataConfigServiceImplTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private SensitiveDataConfigServiceImpl sensitiveDataConfigService;
  private MockConfigServiceImpl mockConfigService = new MockConfigServiceImpl();

  @BeforeEach
  void setUp() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(mockConfigService)
            .build()
            .start());
    ManagedChannel managedChannel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(managedChannel);

    sensitiveDataConfigService = new SensitiveDataConfigServiceImpl(configServiceBlockingStub);
  }

  @Test
  void markParameters() throws InvalidProtocolBufferException {
    Parameter parameter1 = getParameter(ParamType.PARAM_TYPE_BODY, "p1");
    Parameter parameter2 = getParameter(ParamType.PARAM_TYPE_QUERY, "p2");
    MarkParametersRequest request =
        MarkParametersRequest.newBuilder()
            .addAllParameters(List.of(parameter1, parameter2))
            .setEndpoint(ENDPOINT2)
            .setSensitive(true)
            .build();
    Runnable runnable =
        () -> sensitiveDataConfigService.markParameters(request, mock(StreamObserver.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    Value parameterWithSensitivity1 = toValue(getParameterWithSensitivity(parameter1, true));
    Value parameterWithSensitivity2 = toValue(getParameterWithSensitivity(parameter2, true));
    assertEquals(Set.of(parameterWithSensitivity1), mockConfigService.getUpsertedBodyParameters());
    assertEquals(Set.of(parameterWithSensitivity2), mockConfigService.getUpsertedQueryParameters());
  }

  @Test
  void getParameters() {
    StreamObserver<GetParametersResponse> responseObserver = mock(StreamObserver.class);
    GetParametersRequest request =
        GetParametersRequest.newBuilder()
            .setParamType(ParamType.PARAM_TYPE_BODY)
            .setEndpoint(ENDPOINT1)
            .setFilter(Filter.newBuilder().setSensitive(true).build())
            .build();
    Runnable runnable = () -> sensitiveDataConfigService.getParameters(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    ArgumentCaptor<GetParametersResponse> argumentCaptor =
        ArgumentCaptor.forClass(GetParametersResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));

    List<ParameterWithSensitivity> expected =
        getParametersWithSensitivityList().stream()
            .filter(ParameterWithSensitivity::getSensitive)
            .collect(Collectors.toList());
    List<ParameterWithSensitivity> actual =
        argumentCaptor.getValue().getParametersWithSensitivityList();
    assertEquals(expected, actual);
  }

  @Test
  void updateRedactionStrategy() throws InvalidProtocolBufferException {
    Parameter parameter = getParameter(ParamType.PARAM_TYPE_BODY, "p1");
    UpdateRedactionStrategyRequest request =
        UpdateRedactionStrategyRequest.newBuilder()
            .setParameter(parameter)
            .setRedactionStrategy(REDACTION_STRATEGY_HASH)
            .build();
    Runnable runnable =
        () ->
            sensitiveDataConfigService.updateRedactionStrategy(request, mock(StreamObserver.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    PiiFilterConfig upsertedPiiFilterConfig =
        toPiiFilterConfig(mockConfigService.getUpsertedPiiFilterConfig());
    Optional<PiiElement> upsertedPiiElement =
        upsertedPiiFilterConfig.getKeyRegexsList().stream()
            .filter(p -> p.getRegex().equals("p1"))
            .findFirst();
    assertTrue(upsertedPiiElement.isPresent());
    PiiElement expected =
        PiiElement.newBuilder()
            .setRegex("p1")
            .setRedactionStrategy(REDACTION_STRATEGY_HASH)
            .build();
    assertEquals(expected, upsertedPiiElement.get());
  }
}
