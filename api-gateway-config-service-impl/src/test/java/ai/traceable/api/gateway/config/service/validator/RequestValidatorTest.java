package ai.traceable.api.gateway.config.service.validator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
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
}
