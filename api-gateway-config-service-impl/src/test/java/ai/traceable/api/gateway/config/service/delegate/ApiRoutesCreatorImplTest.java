package ai.traceable.api.gateway.config.service.delegate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.NewApiRoute;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import java.util.UUID;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ApiRoutesCreatorImplTest {

  @Mock private ApiRoutesConfigStore mockApiRoutesConfigStore;
  @Mock private UuidGenerator mockUuidGenerator;

  @InjectMocks private ApiRoutesCreatorImpl apiRoutesCreatorImpl;

  @SuppressWarnings("unchecked")
  @Test
  void testCreate() {
    final String orgId = UUID.randomUUID().toString();
    final CreateRoutesRequest request =
        CreateRoutesRequest.newBuilder()
            .addRoutes(
                NewApiRoute.newBuilder()
                    .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
                    .setMetadata(Metadata.newBuilder().setOrgId(orgId))
                    .build())
            .addRoutes(
                NewApiRoute.newBuilder()
                    .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
                    .setMetadata(Metadata.newBuilder().setOrgId(orgId))
                    .build())
            .build();
    final RequestContext requestContext = new RequestContext();
    final String uuid = UUID.randomUUID().toString();

    final ApiRoute apiRoute1 =
        ApiRoute.newBuilder()
            .setId(uuid)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();
    final CreateRoutesResponse expectedResult =
        CreateRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
    when(mockUuidGenerator.generateId(any(NewApiRoute.class))).thenReturn(uuid);
    final ContextualConfigObject<ApiRoute> mockContextualConfigObject =
        mock(ContextualConfigObject.class);
    when(mockContextualConfigObject.getData()).thenReturn(apiRoute1);
    when(mockApiRoutesConfigStore.upsertObjects(requestContext, List.of(apiRoute1)))
        .thenReturn(List.of(mockContextualConfigObject));

    final CreateRoutesResponse result = apiRoutesCreatorImpl.create(request, requestContext);

    assertEquals(expectedResult, result);
  }
}
