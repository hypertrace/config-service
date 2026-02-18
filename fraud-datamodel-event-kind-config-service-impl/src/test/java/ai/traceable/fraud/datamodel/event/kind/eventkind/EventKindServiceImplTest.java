package ai.traceable.fraud.datamodel.event.kind.eventkind;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.GetEventKindsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetEventKindsResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EventKindServiceImplTest {

  @Mock private EventKindProvider provider;
  @Mock private StreamObserver<GetEventKindsResponse> responseObserver;

  private EventKindServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new EventKindServiceImpl(provider);
  }

  @Test
  void getEventKinds_returnsKindsFromProvider() {
    DataModelEventKind kind =
        DataModelEventKind.newBuilder()
            .setId("test_kind")
            .setDisplayName("Test Kind")
            .setDescription("Test description")
            .build();
    when(provider.getEventKinds(any())).thenReturn(List.of(kind));

    GetEventKindsRequest request = GetEventKindsRequest.getDefaultInstance();
    service.getEventKinds(request, responseObserver);

    ArgumentCaptor<GetEventKindsResponse> captor =
        ArgumentCaptor.forClass(GetEventKindsResponse.class);
    verify(responseObserver).onNext(captor.capture());
    verify(responseObserver).onCompleted();

    GetEventKindsResponse response = captor.getValue();
    assertNotNull(response);
    assertEquals(1, response.getEventKindsCount());
    assertEquals("test_kind", response.getEventKinds(0).getId());
  }

  @Test
  void getEventKinds_withFilter_passesFilterToProvider() {
    EventKindFilter filter = EventKindFilter.newBuilder().setSystemOnly(true).build();
    GetEventKindsRequest request = GetEventKindsRequest.newBuilder().setFilter(filter).build();

    when(provider.getEventKinds(filter)).thenReturn(List.of());

    service.getEventKinds(request, responseObserver);

    verify(provider).getEventKinds(filter);
    verify(responseObserver).onCompleted();
  }

  @Test
  void getEventKinds_withNoFilter_usesDefaultFilter() {
    GetEventKindsRequest request = GetEventKindsRequest.getDefaultInstance();
    when(provider.getEventKinds(any())).thenReturn(List.of());

    service.getEventKinds(request, responseObserver);

    verify(provider).getEventKinds(EventKindFilter.getDefaultInstance());
  }
}
