package ai.traceable.saved.filter.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.PublicVisibility;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.Visibility;
import com.google.protobuf.Value;
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
class SavedFilterRequestValidatorTest {

  private static final String TEST_TENANT_ID = "SavedFilterRequestValidatorTest-tenant-id";
  private static final String TEST_USER_ID = "SavedFilterRequestValidatorTest-user-id";
  public static final String TENANT_ID = "Tenant ID";
  public static final String USER_ID = "User ID";
  public static final String VISIBILITY = "visibility";
  public static final String FILTER_CONDITION = "filter condition";

  private final SavedFilterRequestValidator validator = new SavedFilterRequestValidator();
  @Mock private RequestContext mockRequestContext;

  @Test
  void validateCreateSavedFilterRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "CreateSavedFilterRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedFilterRequest.newBuilder().build()));

    assertInvalidArgStatus(
        "CreateSavedFilterRequest.scope",
        () ->
            validator.validateOrThrow(
                mockRequestContext, CreateSavedFilterRequest.newBuilder().setName("f1").build()));

    assertInvalidArgStatus(
        VISIBILITY,
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSavedFilterRequest.newBuilder().setName("f1").setScope("traces").build()));

    assertInvalidArgStatus(
        FILTER_CONDITION,
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSavedFilterRequest.newBuilder()
                    .setName("f1")
                    .setScope("traces")
                    .setVisibility(
                        Visibility.newBuilder()
                            .setPublic(PublicVisibility.newBuilder().build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                CreateSavedFilterRequest.newBuilder()
                    .setName("f1")
                    .setScope("traces")
                    .setVisibility(
                        Visibility.newBuilder()
                            .setPublic(PublicVisibility.newBuilder().build())
                            .build())
                    .setFilterCriteria(buildFilterCriteria())
                    .build()));
  }

  @Test
  void validateUpdateSavedFilterRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "UpdateSavedFilterRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedFilterRequest.newBuilder().build()));

    assertInvalidArgStatus(
        "UpdateSavedFilterRequest.name",
        () ->
            validator.validateOrThrow(
                mockRequestContext, UpdateSavedFilterRequest.newBuilder().setId("id-1").build()));

    assertInvalidArgStatus(
        VISIBILITY,
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSavedFilterRequest.newBuilder().setName("f1").setId("id-1").build()));

    assertInvalidArgStatus(
        FILTER_CONDITION,
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSavedFilterRequest.newBuilder()
                    .setName("f1")
                    .setId("id-1")
                    .setVisibility(
                        Visibility.newBuilder()
                            .setPublic(PublicVisibility.newBuilder().build())
                            .build())
                    .build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                UpdateSavedFilterRequest.newBuilder()
                    .setId("id-1")
                    .setName("f1")
                    .setVisibility(
                        Visibility.newBuilder()
                            .setPublic(PublicVisibility.newBuilder().build())
                            .build())
                    .setFilterCriteria(buildFilterCriteria())
                    .build()));
  }

  @Test
  void validateGetSavedFiltersRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedFiltersRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedFiltersRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "GetSavedFiltersRequest.scope",
        () ->
            validator.validateOrThrow(
                mockRequestContext, GetSavedFiltersRequest.newBuilder().build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext,
                GetSavedFiltersRequest.newBuilder().setScope("traces").build()));
  }

  @Test
  void validateDeleteSavedFilterRequest() {
    assertInvalidArgStatus(
        TENANT_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatus(
        USER_ID,
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedFilterRequest.newBuilder().build()));
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));

    assertInvalidArgStatus(
        "DeleteSavedFilterRequest.id",
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedFilterRequest.newBuilder().setId("").build()));

    assertDoesNotThrow(
        () ->
            validator.validateOrThrow(
                mockRequestContext, DeleteSavedFilterRequest.newBuilder().setId("123").build()));
  }

  private static FilterCriteria buildFilterCriteria() {
    return FilterCriteria.newBuilder()
        .setRelationalFilter(
            RelationalFilterCondition.newBuilder()
                .setFieldName("c1")
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                .setFieldValue(Value.newBuilder().setStringValue("v1").build())
                .build())
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
