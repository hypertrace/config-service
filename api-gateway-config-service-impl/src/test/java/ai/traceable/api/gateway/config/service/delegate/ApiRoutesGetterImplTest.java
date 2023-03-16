package ai.traceable.api.gateway.config.service.delegate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import java.util.List;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiRoutesGetterImplTest {

  @Mock private ApiRoutesConfigStore mockApiRoutesConfigStore;

  @InjectMocks private ApiRoutesGetterImpl apiRoutesGetterImpl;

  @Test
  void testGet() {
    final ApiRouteFilter filter = ApiRouteFilter.newBuilder().build();
    final GetRoutesRequest request = GetRoutesRequest.newBuilder().setFilter(filter).build();
    final RequestContext requestContext = new RequestContext();
    final String uuid = UUID.randomUUID().toString();
    final String orgId = UUID.randomUUID().toString();

    final ApiRoute apiRoute1 =
        ApiRoute.newBuilder()
            .setId(uuid)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();
    final GetRoutesResponse expectedResult =
        GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
    when(mockApiRoutesConfigStore.getAllConfigData(requestContext, filter))
        .thenReturn(List.of(apiRoute1));

    final GetRoutesResponse result = apiRoutesGetterImpl.get(request, requestContext);

    assertEquals(expectedResult, result);
  }
}
