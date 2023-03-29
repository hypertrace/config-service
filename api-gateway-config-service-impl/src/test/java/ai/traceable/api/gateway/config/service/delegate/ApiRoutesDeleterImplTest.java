package ai.traceable.api.gateway.config.service.delegate;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesResponse;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiRoutesDeleterImplTest {

  @Mock private ApiRoutesConfigStore mockApiRoutesConfigStore;

  @InjectMocks private ApiRoutesDeleterImpl apiRoutesDeleterImpl;

  @Test
  void testDelete() {
    final ApiRouteFilter filter = ApiRouteFilter.newBuilder().build();
    final DeleteRoutesRequest request = DeleteRoutesRequest.newBuilder().setFilter(filter).build();
    final RequestContext requestContext = new RequestContext();
    final DeleteRoutesResponse expectedResult = DeleteRoutesResponse.newBuilder().build();

    final String deletableId = "deletableId";
    final List<ApiRoute> apiRoutes = List.of(ApiRoute.newBuilder().setId(deletableId).build());
    when(mockApiRoutesConfigStore.getAllConfigData(requestContext, filter)).thenReturn(apiRoutes);

    when(mockApiRoutesConfigStore.deleteObjects(requestContext, List.of(deletableId)))
        .thenReturn(List.of());

    final DeleteRoutesResponse result = apiRoutesDeleterImpl.delete(request, requestContext);

    assertEquals(expectedResult, result);
    verify(mockApiRoutesConfigStore).getAllConfigData(requestContext, filter);
    verify(mockApiRoutesConfigStore).deleteObjects(requestContext, List.of(deletableId));
  }

  @Test
  void testDelete_ApiRoutesConfigStoreGetAllConfigDataReturnsNoItems() {
    final ApiRouteFilter filter = ApiRouteFilter.newBuilder().build();
    final DeleteRoutesRequest request = DeleteRoutesRequest.newBuilder().setFilter(filter).build();
    final RequestContext requestContext = new RequestContext();
    final DeleteRoutesResponse expectedResult = DeleteRoutesResponse.newBuilder().build();

    when(mockApiRoutesConfigStore.getAllConfigData(requestContext, filter)).thenReturn(emptyList());

    final DeleteRoutesResponse result = apiRoutesDeleterImpl.delete(request, requestContext);

    assertEquals(expectedResult, result);
    verify(mockApiRoutesConfigStore).getAllConfigData(requestContext, filter);
    verify(mockApiRoutesConfigStore, never()).deleteObjects(requestContext, emptyList());
  }
}
