package ai.traceable.api.gateway.config.service.validator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import ai.traceable.api.gateway.config.service.v1.NewApiRoute;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import io.grpc.StatusRuntimeException;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RequestValidatorTest {

  private RequestValidator requestValidator;

  @BeforeEach
  void setUp() {
    requestValidator = new RequestValidator();
  }

  @Nested
  class ValidateCreateRequest {
    @Test
    void testValidateCreateWithoutTenantId() {
      final CreateRoutesRequest request = CreateRoutesRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateCreateWithTenantId() {
      final CreateRoutesRequest request = CreateRoutesRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertDoesNotThrow(() -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateCreateWithoutPath() {
      final CreateRoutesRequest request =
          CreateRoutesRequest.newBuilder()
              .addRoutes(
                  NewApiRoute.newBuilder()
                      .setMetadata(Metadata.newBuilder().setOrgId(UUID.randomUUID().toString())))
              .build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateCreateWithoutOrgId() {
      final CreateRoutesRequest request =
          CreateRoutesRequest.newBuilder()
              .addRoutes(
                  NewApiRoute.newBuilder().setInfo(RouteInfo.newBuilder().setPath("/hello/Mars")))
              .build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }
  }

  @Nested
  class ValidateGetRequest {
    @Test
    void testValidateGetWithoutTenantId() {
      final GetRoutesRequest request = GetRoutesRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateGetWithTenantId() {
      final GetRoutesRequest request = GetRoutesRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertDoesNotThrow(() -> requestValidator.validate(request, requestContext));
    }
  }

  @Nested
  class ValidateDeleteRequest {
    @Test
    void testValidateDeleteWithoutTenantId() {
      final DeleteRoutesRequest request = DeleteRoutesRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutFilter() {
      final DeleteRoutesRequest request = DeleteRoutesRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutOrgIdFilter() {
      final DeleteRoutesRequest request =
          DeleteRoutesRequest.newBuilder().setFilter(ApiRouteFilter.newBuilder()).build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutOrgId() {
      final DeleteRoutesRequest request =
          DeleteRoutesRequest.newBuilder()
              .setFilter(ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder()))
              .build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithOrgIds() {
      final DeleteRoutesRequest request =
          DeleteRoutesRequest.newBuilder()
              .setFilter(
                  ApiRouteFilter.newBuilder()
                      .setOrgIds(OrgIds.newBuilder().addOrgId(UUID.randomUUID().toString())))
              .build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertDoesNotThrow(() -> requestValidator.validate(request, requestContext));
    }
  }

  @Nested
  class ValidateMetadataCreateRequest {
    @Test
    void testValidateCreateWithoutTenantId() {
      final CreateMetadataRequest request = CreateMetadataRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateCreateWithoutOrgId() {
      final CreateMetadataRequest request = CreateMetadataRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }
  }

  @Nested
  class ValidateGetMetadataRequest {
    @Test
    void testValidateGetWithoutTenantId() {
      final GetMetadataRequest request = GetMetadataRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateGetWithTenantId() {
      final GetMetadataRequest request = GetMetadataRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertDoesNotThrow(() -> requestValidator.validate(request, requestContext));
    }
  }

  @Nested
  class ValidateDeleteMetadataRequest {
    @Test
    void testValidateDeleteWithoutTenantId() {
      final DeleteMetadataRequest request = DeleteMetadataRequest.newBuilder().build();
      final RequestContext requestContext = new RequestContext();

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutFilter() {
      final DeleteMetadataRequest request = DeleteMetadataRequest.newBuilder().build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutOrgIdFilter() {
      final DeleteMetadataRequest request =
          DeleteMetadataRequest.newBuilder().setMetadataFilter(MetadataFilter.newBuilder()).build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithoutOrgId() {
      final DeleteMetadataRequest request =
          DeleteMetadataRequest.newBuilder()
              .setMetadataFilter(MetadataFilter.newBuilder().setOrgIds(OrgIds.newBuilder()))
              .build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertThrows(
          StatusRuntimeException.class, () -> requestValidator.validate(request, requestContext));
    }

    @Test
    void testValidateDeleteWithOrgIds() {
      final DeleteMetadataRequest request =
          DeleteMetadataRequest.newBuilder()
              .setMetadataFilter(
                  MetadataFilter.newBuilder()
                      .setOrgIds(OrgIds.newBuilder().addOrgId(UUID.randomUUID().toString())))
              .build();
      final RequestContext requestContext = RequestContext.forTenantId("tenant-id");

      assertDoesNotThrow(() -> requestValidator.validate(request, requestContext));
    }
  }
}
