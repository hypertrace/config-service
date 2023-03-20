package ai.traceable.api.gateway.config.cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.v1.ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceBlockingStub;
import ai.traceable.api.gateway.config.service.v1.ApiInfo;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import ai.traceable.platform.event.invalidation.cache.ChangeEventConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import java.util.List;
import java.util.UUID;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultCachedApiRouteProviderTest {
  private static final String API_PATH1 = "/planets/Mars";
  private static final String API_PATH2 = "/planets/The_Red_Planet";

  private static final String ORG_ID1 = "org1-id";
  private static final String ORG_ID2 = "org2-id";

  private static final String SERVICE1 = "Service 1";
  private static final String SERVICE2 = "Service 2";

  private static final String METHOD1 = "GET";
  private static final String METHOD2 = "POST";

  private static final ApiRoute ROUTE_WITH_ALL_FIELDS =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID1))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH1)
                  .setHttpMethod(METHOD1)
                  .setServiceName(SERVICE1)
                  .setIsDeprecated(true))
          .build();

  private static final ApiRoute ROUTE_WITHOUT_METHOD =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID1))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH2)
                  .setServiceName(SERVICE2)
                  .setIsDeprecated(true))
          .build();

  private static final ApiRoute ROUTE_WITHOUT_SERVICE =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID2))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH1)
                  .setHttpMethod(METHOD2)
                  .setIsDeprecated(false))
          .build();

  private static final String CUSTOMER_ID = "a-tenant-from-Mars";
  private static final String ANOTHER_CUSTOMER_ID = "a-different-tenant-from-Mars";
  private static final RequestContext context = RequestContext.forTenantId(CUSTOMER_ID);

  @Mock private ApiGatewayConfigServiceBlockingStub mockGatewayConfigServiceClient;
  @Mock private ChangeEventConsumer<String, List<ApiRoute>> mockChangeEventConsumer;
  private DefaultCachedApiRouteProvider apiRouteProvider;

  @BeforeEach
  void setup() {
    final Config config =
        ConfigFactory.parseResources("api-gateway-cache-test.conf").getConfig("api.gateway.config");
    final GatewayConfigServiceCacheConfig gatewayConfig =
        GatewayConfigServiceCacheConfig.builder(config)
            .callCredentials(mock(CallCredentials.class))
            .channel(mock(Channel.class))
            .build();
    apiRouteProvider =
        new DefaultCachedApiRouteProvider(
            gatewayConfig, mockChangeEventConsumer, mockGatewayConfigServiceClient);

    when(mockGatewayConfigServiceClient.withDeadlineAfter(10_000, MILLISECONDS))
        .thenReturn(mockGatewayConfigServiceClient);
    when(mockGatewayConfigServiceClient.getRoutes(GetRoutesRequest.getDefaultInstance()))
        .thenReturn(
            GetRoutesResponse.newBuilder()
                .addRoutes(ROUTE_WITH_ALL_FIELDS)
                .addRoutes(ROUTE_WITHOUT_SERVICE)
                .addRoutes(ROUTE_WITHOUT_METHOD)
                .build());
    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder()
                .setApiInfo(ApiInfo.newBuilder().setPath(API_PATH1 + "/the_south_pole"))
                .build(),
            context);
    // This 'assert' in the setup method is intended
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_SERVICE), routes);
    verifyInteractionsWithClient(1);
  }

  @Test
  void getWithoutFilter_makesTheServiceCallJustOnce() {
    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(ApiRouteFilter.getDefaultInstance(), context);
    assertEquals(
        List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_SERVICE, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(1);
  }

  @Test
  void getWithDifferentFilter_makesTheServiceCallJustOnce() {
    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build(),
            context);
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(1);
  }

  @Test
  void waitForExpiryThenGet_makesTheServiceCallTwice() throws InterruptedException {
    SECONDS.sleep(3);

    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build(),
            context);
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(2);
  }

  @Test
  void invalidateAndThenGet_makesTheServiceCallTwice() {
    apiRouteProvider.invalidateCache(
        ConfigChangeEventKey.newBuilder()
            .setTenantId(CUSTOMER_ID)
            .setConfigType(ApiRoute.class.getName())
            .build(),
        ConfigChangeEventValue.newBuilder().build());

    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build(),
            context);
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(2);
  }

  @Test
  void invalidateForDifferentTenantAndThenGet_makesTheServiceCallJustOnce() {
    apiRouteProvider.invalidateCache(
        ConfigChangeEventKey.newBuilder()
            .setTenantId(ANOTHER_CUSTOMER_ID)
            .setConfigType(ApiRoute.class.getName())
            .build(),
        ConfigChangeEventValue.newBuilder().build());

    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build(),
            context);
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(1);
  }

  @Test
  void invalidateForDifferentConfigTypeAndThenGet_makesTheServiceCallJustOnce() {
    apiRouteProvider.invalidateCache(
        ConfigChangeEventKey.newBuilder()
            .setTenantId(CUSTOMER_ID)
            .setConfigType(OrgIds.class.getName())
            .build(),
        ConfigChangeEventValue.newBuilder().build());

    final List<ApiRoute> routes =
        apiRouteProvider.getRoutes(
            ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build(),
            context);
    assertEquals(List.of(ROUTE_WITH_ALL_FIELDS, ROUTE_WITHOUT_METHOD), routes);

    verifyInteractionsWithClient(1);
  }

  private void verifyInteractionsWithClient(final int numExpectedInteractions) {
    verify(mockGatewayConfigServiceClient, times(numExpectedInteractions))
        .withDeadlineAfter(10_000, MILLISECONDS);
    verify(mockGatewayConfigServiceClient, times(numExpectedInteractions))
        .getRoutes(GetRoutesRequest.getDefaultInstance());
    verifyNoMoreInteractions(mockGatewayConfigServiceClient);
  }
}
