package ai.traceable.saved.query.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesFilter;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.QueryClauses;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SavedQueryRequestValidatorTest {

  private static final String TEST_TENANT_ID = "SavedQueryRequestValidatorTest-tenant-id";
  private static final String TEST_USER_ID = "SavedQueryRequestValidatorTest-user-id";
  public static final String TENANT_ID = "Tenant ID";
  public static final String USER_ID = "User ID";
  private final SavedQueryRequestValidator validator = new SavedQueryRequestValidator();
  @Mock private RequestContext mockRequestContext;

  @Test
  void validateCreateSavedQueryRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "CreateSavedQueryRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedQueryRequest.newBuilder().build()));

    assertInvalidArgStatus(
        "CreateSavedQueryRequest.scope",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedQueryRequest.newBuilder().setName("f1").build()));

    assertInvalidArgStatus(
        "QueryClauses.selection",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSavedQueryRequest.newBuilder().setName("f1").setScope("traces").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSavedQueryRequest.newBuilder()
                    .setName("f1")
                    .setScope("traces")
                    .setQueryClauses(buildQueryClauses())
                    .build()));
  }

  @Test
  void validateUpdateSavedQueryRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "UpdateSavedQueryRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedQueryRequest.newBuilder().build()));

    assertInvalidArgStatus(
        "UpdateSavedQueryRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedQueryRequest.newBuilder().setId("id-1").build()));

    assertInvalidArgStatus(
        "QueryClauses.selection",
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSavedQueryRequest.newBuilder().setName("f1").setId("id-1").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSavedQueryRequest.newBuilder()
                    .setId("id-1")
                    .setName("f1")
                    .setQueryClauses(buildQueryClauses())
                    .build()));
  }

  @Test
  void validateGetSavedQueriesRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedQueriesRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedQueriesRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedQueriesRequest.newBuilder().build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                GetSavedQueriesRequest.newBuilder()
                    .setFilter(GetSavedQueriesFilter.newBuilder().setScope("traces").build())
                    .build()));
  }

  @Test
  void validateDeleteSavedQueryRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedQueryRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "DeleteSavedQueryRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedQueryRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedQueryRequest.newBuilder().setId("123").build()));
  }

  private static QueryClauses buildQueryClauses() {
    return QueryClauses.newBuilder()
        .addSelection("column:discountcount(userId)")
        .addGroupBy("country")
        .build();
  }

  private void assertInvalidArgStatus(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
