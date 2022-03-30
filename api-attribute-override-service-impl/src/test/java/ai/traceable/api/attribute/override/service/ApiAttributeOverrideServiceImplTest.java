package ai.traceable.api.attribute.override.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.api.attribute.override.service.handlers.GetAllApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.handlers.RemoveApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.handlers.UpsertApiAttributeOverridesHandler;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiAttributeOverrideServiceImplTest {
  private ApiAttributeOverrideServiceImpl serviceImpl;
  private GetAllApiAttributeOverridesHandler getAllApiAttributeOverridesHandler;
  private UpsertApiAttributeOverridesHandler upsertApiAttributeOverridesHandler;
  private RemoveApiAttributeOverridesHandler removeApiAttributeOverridesHandler;
  private ApiAttributeOverrideRequestValidator requestValidator;

  @BeforeEach
  void setup() {
    getAllApiAttributeOverridesHandler = mock(GetAllApiAttributeOverridesHandler.class);
    upsertApiAttributeOverridesHandler = mock(UpsertApiAttributeOverridesHandler.class);
    removeApiAttributeOverridesHandler = mock(RemoveApiAttributeOverridesHandler.class);
    requestValidator = mock(ApiAttributeOverrideRequestValidator.class);
    serviceImpl =
        new ApiAttributeOverrideServiceImpl(
            requestValidator,
            getAllApiAttributeOverridesHandler,
            upsertApiAttributeOverridesHandler,
            removeApiAttributeOverridesHandler);
  }

  @Test
  void testGetAllAttributeOverrides() {
    GetAllApiAttributeOverridesRequest request =
        GetAllApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(GetAllApiAttributeOverridesRequest.class)))
        .thenReturn(Status.OK);
    StreamObserver streamObserver = mock(StreamObserver.class);
    serviceImpl.getAllApiAttributeOverrides(request, streamObserver);

    verify(getAllApiAttributeOverridesHandler).getOverrides(any(), any());
    verify(streamObserver).onNext(any());
    verify(streamObserver).onCompleted();
  }

  @Test
  void testGetAllAttributeOverrides_invalidRequest() {
    GetAllApiAttributeOverridesRequest request =
        GetAllApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(GetAllApiAttributeOverridesRequest.class)))
        .thenReturn(Status.INVALID_ARGUMENT);
    StreamObserver streamObserver = mock(StreamObserver.class);
    serviceImpl.getAllApiAttributeOverrides(request, streamObserver);

    verify(getAllApiAttributeOverridesHandler, never()).getOverrides(any(), any());
    verify(streamObserver).onError(any());
  }

  @Test
  void testGetAllAttributeOverrides_error() {
    GetAllApiAttributeOverridesRequest request =
        GetAllApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(GetAllApiAttributeOverridesRequest.class)))
        .thenReturn(Status.OK);
    StreamObserver streamObserver = mock(StreamObserver.class);
    when(getAllApiAttributeOverridesHandler.getOverrides(any(), any()))
        .thenThrow(new IllegalArgumentException());
    serviceImpl.getAllApiAttributeOverrides(request, streamObserver);
    verify(getAllApiAttributeOverridesHandler).getOverrides(any(), any());
    verify(streamObserver).onError(any());
  }

  @Test
  void testUpsertAttributeOverrides() {
    UpsertApiAttributeOverridesRequest request =
        UpsertApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(UpsertApiAttributeOverridesRequest.class)))
        .thenReturn(Status.OK);
    StreamObserver streamObserver = mock(StreamObserver.class);
    when(upsertApiAttributeOverridesHandler.upsertAttributeOverrides(any(), any()))
        .thenReturn(ApiAttributeOverrides.getDefaultInstance());
    serviceImpl.upsertApiAttributeOverrides(request, streamObserver);

    verify(upsertApiAttributeOverridesHandler).upsertAttributeOverrides(any(), any());
    verify(streamObserver).onNext(any());
    verify(streamObserver).onCompleted();
  }

  @Test
  void testUpsertAttributeOverrides_invalidRequest() {
    UpsertApiAttributeOverridesRequest request =
        UpsertApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(UpsertApiAttributeOverridesRequest.class)))
        .thenReturn(Status.INVALID_ARGUMENT);
    StreamObserver streamObserver = mock(StreamObserver.class);
    when(upsertApiAttributeOverridesHandler.upsertAttributeOverrides(any(), any()))
        .thenReturn(ApiAttributeOverrides.getDefaultInstance());
    serviceImpl.upsertApiAttributeOverrides(request, streamObserver);

    verify(upsertApiAttributeOverridesHandler, never()).upsertAttributeOverrides(any(), any());
    verify(streamObserver).onError(any());
  }

  @Test
  void testUpsertAttributeOverrides_error() {
    UpsertApiAttributeOverridesRequest request =
        UpsertApiAttributeOverridesRequest.getDefaultInstance();
    when(requestValidator.validate(any(UpsertApiAttributeOverridesRequest.class)))
        .thenReturn(Status.OK);
    StreamObserver streamObserver = mock(StreamObserver.class);
    when(upsertApiAttributeOverridesHandler.upsertAttributeOverrides(any(), any()))
        .thenThrow(new IllegalArgumentException());
    serviceImpl.upsertApiAttributeOverrides(request, streamObserver);

    verify(upsertApiAttributeOverridesHandler).upsertAttributeOverrides(any(), any());
    verify(streamObserver).onError(any());
  }
}
