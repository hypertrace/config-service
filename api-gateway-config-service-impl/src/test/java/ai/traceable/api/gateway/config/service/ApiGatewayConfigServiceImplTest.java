package ai.traceable.api.gateway.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.quality.Strictness.LENIENT;

import ai.traceable.api.gateway.config.service.delegate.ApiRoutesCreatorImpl;
import ai.traceable.api.gateway.config.service.delegate.ApiRoutesDeleterImpl;
import ai.traceable.api.gateway.config.service.delegate.ApiRoutesGetterImpl;
import ai.traceable.api.gateway.config.service.filter.ApiRouteFilterModule;
import ai.traceable.api.gateway.config.service.filter.ApiRouteFilterToPredicateConverter;
import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiGatewayConfigServiceGrpc;
import ai.traceable.api.gateway.config.service.v1.ApiInfo;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.NewApiRoute;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import ai.traceable.api.gateway.config.service.validator.RequestValidator;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
@TestInstance(Lifecycle.PER_CLASS)
class ApiGatewayConfigServiceImplTest {
  private ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceBlockingStub apiGatewayConfigService;

  @SuppressWarnings("Convert2Diamond")
  @BeforeAll
  void setup() {
    final MockGenericConfigService mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockDeleteAll()
            .mockUpsertAll();

    final ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    final ApiRoutesConfigStore apiRoutesConfigStore =
        new ApiRoutesConfigStore(
            genericStub,
            mock(ConfigChangeEventGenerator.class),
            Guice.createInjector(new ApiRouteFilterModule())
                .getInstance(
                    Key.get(
                        new TypeLiteral<
                            Map<
                                ApiRouteFilter.TypeCase,
                                ApiRouteFilterToPredicateConverter>>() {})));

    mockGenericConfigService
        .addService(
            new ApiGatewayConfigServiceImpl(
                new RequestValidator(),
                new ApiRoutesCreatorImpl(apiRoutesConfigStore, new UuidGenerator()),
                new ApiRoutesGetterImpl(apiRoutesConfigStore),
                new ApiRoutesDeleterImpl(apiRoutesConfigStore)))
        .start();

    apiGatewayConfigService =
        ApiGatewayConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void testCreateRoutes() {
    final String uuid = "992bfdcc-a5ab-52da-8d53-f7c2a1be2c2e";
    final String orgId = "15bb0ac4-4d0e-436e-a0ee-d0b03902cc0f";

    final NewApiRoute newApiRoute =
        NewApiRoute.newBuilder()
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();

    final ApiRoute apiRoute =
        ApiRoute.newBuilder()
            .setId(uuid)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();

    final CreateRoutesRequest request =
        CreateRoutesRequest.newBuilder().addRoutes(newApiRoute).build();
    final CreateRoutesResponse expectedResponse =
        CreateRoutesResponse.newBuilder().addRoutes(apiRoute).build();

    final CreateRoutesResponse response = apiGatewayConfigService.createRoutes(request);

    assertEquals(expectedResponse, response);
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class GetRoutesTest {
    private final String id1 = "992bfdcc-a5ab-52da-8d53-f7c2a1be2c2e";
    private final String id2 = "dc8d59ba-4d05-5976-adf7-2b79feab4524";
    private final String orgId = "15bb0ac4-4d0e-436e-a0ee-d0b03902cc0f";
    private final String orgId2 = "d0b03902-4d0e-436e-a0ee-cc15bb0ac40f";
    private final String nonExistingOrgId = "2d87b246-5031-42c0-a914-2e99944186cc";

    private final ApiRoute apiRoute1 =
        ApiRoute.newBuilder()
            .setId(id1)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();

    private final ApiRoute apiRoute2 =
        ApiRoute.newBuilder()
            .setId(id2)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/The_Red_Planet").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId2))
            .build();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeAll
    void setupForGet() {
      final NewApiRoute newApiRoute1 =
          NewApiRoute.newBuilder()
              .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
              .setMetadata(Metadata.newBuilder().setOrgId(orgId))
              .build();

      final NewApiRoute newApiRoute2 =
          NewApiRoute.newBuilder()
              .setInfo(
                  RouteInfo.newBuilder().setPath("/planet/The_Red_Planet").setIsDeprecated(true))
              .setMetadata(Metadata.newBuilder().setOrgId(orgId2))
              .build();

      final CreateRoutesRequest request =
          CreateRoutesRequest.newBuilder().addRoutes(newApiRoute1).addRoutes(newApiRoute2).build();
      apiGatewayConfigService.createRoutes(request);
    }

    @Test
    void testGetRoutesWithoutFiltering() {
      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithOrgIdsFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setOrgIds(
                          OrgIds.newBuilder()
                              .addOrgId(orgId)
                              .addOrgId(orgId2)
                              .addOrgId(nonExistingOrgId)))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithOrgIdFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(orgId)))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithNonExistingOrgIdFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setOrgIds(OrgIds.newBuilder().addOrgId(nonExistingOrgId)))
              .build();
      final GetRoutesResponse expectedResponse = GetRoutesResponse.newBuilder().build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithApiPathExactFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setApiInfo(ApiInfo.newBuilder().setPath("/planet/Mars")))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithApiPathPrefixFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setApiInfo(ApiInfo.newBuilder().setPath("/planet/Mars/south_pole")))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithPathAndServiceNameFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setApiInfo(
                          ApiInfo.newBuilder()
                              .setPath("/planet/Mars/south_pole")
                              .setServiceName("new_service")))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testGetRoutesWithPathServiceNameAndMethodFilter() {
      final GetRoutesRequest getRequest =
          GetRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setApiInfo(
                          ApiInfo.newBuilder()
                              .setPath("/planet/Mars/south_pole")
                              .setServiceName("new_service")
                              .setHttpMethod("PUT")))
              .build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class DeleteRoutesTest {
    private final String id1 = "992bfdcc-a5ab-52da-8d53-f7c2a1be2c2e";
    private final String id2 = "dc8d59ba-4d05-5976-adf7-2b79feab4524";
    private final String orgId = "15bb0ac4-4d0e-436e-a0ee-d0b03902cc0f";
    private final String orgId2 = "d0b03902-4d0e-436e-a0ee-cc15bb0ac40f";
    private final String nonExistingOrgId = "2d87b246-5031-42c0-a914-2e99944186cc";

    private final ApiRoute apiRoute1 =
        ApiRoute.newBuilder()
            .setId(id1)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId))
            .build();

    private final ApiRoute apiRoute2 =
        ApiRoute.newBuilder()
            .setId(id2)
            .setInfo(RouteInfo.newBuilder().setPath("/planet/The_Red_Planet").setIsDeprecated(true))
            .setMetadata(Metadata.newBuilder().setOrgId(orgId2))
            .build();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    void setupForDelete() {
      final NewApiRoute newApiRoute1 =
          NewApiRoute.newBuilder()
              .setInfo(RouteInfo.newBuilder().setPath("/planet/Mars").setIsDeprecated(true))
              .setMetadata(Metadata.newBuilder().setOrgId(orgId))
              .build();

      final NewApiRoute newApiRoute2 =
          NewApiRoute.newBuilder()
              .setInfo(
                  RouteInfo.newBuilder().setPath("/planet/The_Red_Planet").setIsDeprecated(true))
              .setMetadata(Metadata.newBuilder().setOrgId(orgId2))
              .build();

      final CreateRoutesRequest request =
          CreateRoutesRequest.newBuilder().addRoutes(newApiRoute1).addRoutes(newApiRoute2).build();
      apiGatewayConfigService.createRoutes(request);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testDeleteRoutesWithoutFiltering() {
      assertThrows(
          StatusRuntimeException.class,
          () -> apiGatewayConfigService.deleteRoutes(DeleteRoutesRequest.newBuilder().build()));

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testDeleteRoutesWithoutOrgIds() {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiGatewayConfigService.deleteRoutes(
                  DeleteRoutesRequest.newBuilder().setFilter(ApiRouteFilter.newBuilder()).build()));

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testDeleteRoutesWithEmptyOrgIds() {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiGatewayConfigService.deleteRoutes(
                  DeleteRoutesRequest.newBuilder()
                      .setFilter(ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder()))
                      .build()));

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testDeleteRoutesWithOrgIdsFilter() {
      final DeleteRoutesResponse deleteRoutesResponse =
          apiGatewayConfigService.deleteRoutes(
              DeleteRoutesRequest.newBuilder()
                  .setFilter(
                      ApiRouteFilter.newBuilder()
                          .setOrgIds(
                              OrgIds.newBuilder()
                                  .addOrgId(orgId2)
                                  .addOrgId(orgId)
                                  .addOrgId(nonExistingOrgId)))
                  .build());
      assertEquals(DeleteRoutesResponse.newBuilder().build(), deleteRoutesResponse);

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse = GetRoutesResponse.newBuilder().build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testDeleteRoutesWithOrgIdFilter() {
      final DeleteRoutesResponse deleteRoutesResponse =
          apiGatewayConfigService.deleteRoutes(
              DeleteRoutesRequest.newBuilder()
                  .setFilter(
                      ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(orgId2)))
                  .build());
      assertEquals(DeleteRoutesResponse.newBuilder().build(), deleteRoutesResponse);

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }

    @Test
    void testDeleteRoutesWithNonExistingOrgIdFilter() {
      final DeleteRoutesResponse deleteRoutesResponse =
          apiGatewayConfigService.deleteRoutes(
              DeleteRoutesRequest.newBuilder()
                  .setFilter(
                      ApiRouteFilter.newBuilder()
                          .setOrgIds(OrgIds.newBuilder().addOrgId(nonExistingOrgId)))
                  .build());
      assertEquals(DeleteRoutesResponse.newBuilder().build(), deleteRoutesResponse);

      final GetRoutesRequest getRequest = GetRoutesRequest.newBuilder().build();
      final GetRoutesResponse expectedResponse =
          GetRoutesResponse.newBuilder().addRoutes(apiRoute2).addRoutes(apiRoute1).build();
      final GetRoutesResponse response = apiGatewayConfigService.getRoutes(getRequest);
      assertEquals(expectedResponse, response);
    }
  }
}
