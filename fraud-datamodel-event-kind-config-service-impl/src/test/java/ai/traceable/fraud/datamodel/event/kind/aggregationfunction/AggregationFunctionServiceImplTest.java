package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.GetAggregationFunctionsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetAggregationFunctionsResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AggregationFunctionServiceImplTest {

  @Mock private AggregationFunctionProvider provider;
  @Mock private StreamObserver<GetAggregationFunctionsResponse> responseObserver;

  private AggregationFunctionServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new AggregationFunctionServiceImpl(provider);
  }

  @Test
  void getAggregationFunctions_withEmptyFilter_returnsValidationError() {
    GetAggregationFunctionsRequest request = GetAggregationFunctionsRequest.getDefaultInstance();

    service.getAggregationFunctions(request, responseObserver);

    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(captor.capture());
    verify(responseObserver, never()).onNext(any());
    verify(provider, never()).getFunctionsByKinds(any());

    Throwable error = captor.getValue();
    assertTrue(error instanceof StatusRuntimeException);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
    assertTrue(
        ((StatusRuntimeException) error)
            .getStatus()
            .getDescription()
            .contains("At least 1 compatible_with_kinds must be provided"));
  }

  @Test
  void getAggregationFunctions_withKinds_returnsMappings() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    AggregationFunctionFilter filter =
        AggregationFunctionFilter.newBuilder().addCompatibleWithKinds(numericKind).build();
    GetAggregationFunctionsRequest request =
        GetAggregationFunctionsRequest.newBuilder().setFilter(filter).build();

    AggregationFunction func =
        AggregationFunction.newBuilder()
            .setFunctionType(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
            .setDisplayName("Test")
            .setDescription("Test function")
            .build();
    AggregationFunctionsByKind mapping =
        AggregationFunctionsByKind.newBuilder().setKind(numericKind).addFunctions(func).build();

    when(provider.getFunctionsByKinds(List.of(numericKind))).thenReturn(List.of(mapping));

    service.getAggregationFunctions(request, responseObserver);

    ArgumentCaptor<GetAggregationFunctionsResponse> captor =
        ArgumentCaptor.forClass(GetAggregationFunctionsResponse.class);
    verify(responseObserver).onNext(captor.capture());
    verify(responseObserver).onCompleted();

    GetAggregationFunctionsResponse response = captor.getValue();
    assertEquals(1, response.getFunctionMappingsCount());
    assertEquals(numericKind, response.getFunctionMappings(0).getKind());
  }

  @Test
  void getAggregationFunctions_withNoFilter_returnsValidationError() {
    GetAggregationFunctionsRequest request = GetAggregationFunctionsRequest.getDefaultInstance();

    service.getAggregationFunctions(request, responseObserver);

    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(captor.capture());

    Throwable error = captor.getValue();
    assertTrue(error instanceof StatusRuntimeException);
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(), ((StatusRuntimeException) error).getStatus().getCode());
  }
}
