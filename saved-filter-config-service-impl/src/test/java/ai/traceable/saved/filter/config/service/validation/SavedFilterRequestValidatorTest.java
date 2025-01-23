package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.ArrayOperator.ARRAY_OPERATOR_ALL;
import static ai.traceable.saved.filter.config.service.v1.ArrayOperator.ARRAY_OPERATOR_ANY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.saved.filter.config.service.store.SavedFilterStoreManager;
import ai.traceable.saved.filter.config.service.v1.ArrayFilterCondition;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.PublicVisibility;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.Visibility;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
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

  private SavedFilterRequestValidator validator;
  @Mock private RequestContext mockRequestContext;
  @Mock private SavedFilterStoreManager mockSavedFilterStoreManager;
  @Mock private AttributeServiceCachedClient mockAttributeServiceCachedClient;

  @BeforeEach
  void setup() {

    Injector injector =
        Guice.createInjector(
            Modules.override(new SavedFilterValidationModule())
                .with(
                    new AbstractModule() {
                      @Override
                      protected void configure() {
                        bind(AttributeServiceCachedClient.class)
                            .toInstance(mockAttributeServiceCachedClient);
                      }
                    }));
    SavedFilterValidator<FilterCriteria> savedFilterValidator =
        injector.getInstance(Key.get(new TypeLiteral<SavedFilterValidator<FilterCriteria>>() {}));
    validator = new SavedFilterRequestValidator(savedFilterValidator, mockSavedFilterStoreManager);
  }

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
  void testFilterConditionNotPresent() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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

    assertEquals(
        "UNIMPLEMENTED: Filter type FILTERCONDITION_NOT_SET is not supported",
        statusRuntimeException.getMessage());
  }

  @Test
  void testFilterConditionWithInvalidAttribute() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "invalidKey"))
        .thenReturn(Optional.empty());

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setRelationalFilter(
                                    RelationalFilterCondition.newBuilder()
                                        .setLhsExpression(
                                            Expression.newBuilder()
                                                .setField(Field.newBuilder().setKey("invalidKey"))
                                                .build())
                                        .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                        .setRhsExpression(
                                            Expression.newBuilder()
                                                .setValue(
                                                    Value.newBuilder().setStringValue("v1").build())
                                                .build())
                                        .build()))
                        .build()));

    assertEquals(
        "INVALID_ARGUMENT: Invalid attribute key (invalidKey) with scope (traces)",
        statusRuntimeException.getMessage());
  }

  @Test
  void testFilterConditionWithNullRhs() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING).build()));

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setRelationalFilter(
                                    RelationalFilterCondition.newBuilder()
                                        .setLhsExpression(
                                            Expression.newBuilder()
                                                .setField(Field.newBuilder().setKey("validKey"))
                                                .build())
                                        .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                        .setRhsExpression(
                                            Expression.newBuilder()
                                                .setValue(
                                                    Value.newBuilder()
                                                        .setNullValue(NullValue.NULL_VALUE))
                                                .build())
                                        .build()))
                        .build()));
    String actualMessage = statusRuntimeException.getMessage();
    assertTrue(
        actualMessage.equals("INVALID_ARGUMENT: Value cannot contains: [NULL_VALUE, STRUCT_VALUE]")
            || actualMessage.equals(
                "INVALID_ARGUMENT: Value cannot contains: [STRUCT_VALUE, NULL_VALUE]"),
        "Exception message should match either of the expected descriptions");
  }

  @Test
  void testFilterConditionWithEmptyList() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING).build()));

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setRelationalFilter(
                                    RelationalFilterCondition.newBuilder()
                                        .setLhsExpression(
                                            Expression.newBuilder()
                                                .setField(Field.newBuilder().setKey("validKey"))
                                                .build())
                                        .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                        .setRhsExpression(
                                            Expression.newBuilder()
                                                .setValue(
                                                    Value.newBuilder()
                                                        .setListValue(
                                                            ListValue.getDefaultInstance()))
                                                .build())
                                        .build()))
                        .build()));

    assertEquals(
        "INVALID_ARGUMENT: List values cannot be empty", statusRuntimeException.getMessage());
  }

  @Test
  void testFilterConditionWithNonEmptyList() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING).build()));

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
                    .setFilterCriteria(
                        FilterCriteria.newBuilder()
                            .setRelationalFilter(
                                RelationalFilterCondition.newBuilder()
                                    .setLhsExpression(
                                        Expression.newBuilder()
                                            .setField(Field.newBuilder().setKey("validKey"))
                                            .build())
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_NOT_IN)
                                    .setRhsExpression(
                                        Expression.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setStringValue("stringValue")
                                                                    .build())))
                                            .build())
                                    .build()))
                    .build()));
  }

  @Test
  void testFilterConditionWithValidAttribute() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING).build()));

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
                    .setFilterCriteria(
                        FilterCriteria.newBuilder()
                            .setRelationalFilter(
                                RelationalFilterCondition.newBuilder()
                                    .setLhsExpression(
                                        Expression.newBuilder()
                                            .setField(Field.newBuilder().setKey("validKey"))
                                            .build())
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                    .setRhsExpression(
                                        Expression.newBuilder()
                                            .setValue(
                                                Value.newBuilder().setStringValue("v1").build())
                                            .build())
                                    .build()))
                    .build()));
  }

  @Test
  void testValidArrayFilterConditionWithSingleValue() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING_ARRAY).build()));

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
                    .setFilterCriteria(
                        FilterCriteria.newBuilder()
                            .setArrayFilter(
                                ArrayFilterCondition.newBuilder()
                                    .setOperator(ARRAY_OPERATOR_ANY)
                                    .setRelationalFilter(
                                        RelationalFilterCondition.newBuilder()
                                            .setLhsExpression(
                                                Expression.newBuilder()
                                                    .setField(
                                                        Field.newBuilder().setKey("validKey")))
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_GREATER_THAN)
                                            .setRhsExpression(
                                                Expression.newBuilder()
                                                    .setValue(
                                                        Value.newBuilder().setStringValue("v1"))))))
                    .build()));
  }

  @Test
  void testValidArrayFilterConditionWithIncompatibleTypes() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_INT64_ARRAY).build()));

    final StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setArrayFilter(
                                    ArrayFilterCondition.newBuilder()
                                        .setOperator(ARRAY_OPERATOR_ANY)
                                        .setRelationalFilter(
                                            RelationalFilterCondition.newBuilder()
                                                .setLhsExpression(
                                                    Expression.newBuilder()
                                                        .setField(
                                                            Field.newBuilder().setKey("validKey")))
                                                .setOperator(
                                                    RelationalOperator
                                                        .RELATIONAL_OPERATOR_GREATER_THAN)
                                                .setRhsExpression(
                                                    Expression.newBuilder()
                                                        .setValue(
                                                            Value.newBuilder()
                                                                .setStringValue("v1"))))))
                        .build()));

    assertEquals(
        "INVALID_ARGUMENT: Incompatible attribute kind for operator 'RELATIONAL_OPERATOR_GREATER_THAN': LHS is of type TYPE_INT64 and derived RHS is of type TYPE_STRING",
        statusRuntimeException.getMessage());
  }

  @Test
  void testValidArrayFilterConditionWithoutOperator() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    final StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setArrayFilter(
                                    ArrayFilterCondition.newBuilder()
                                        .setRelationalFilter(
                                            RelationalFilterCondition.newBuilder()
                                                .setLhsExpression(
                                                    Expression.newBuilder()
                                                        .setField(
                                                            Field.newBuilder().setKey("validKey")))
                                                .setOperator(
                                                    RelationalOperator
                                                        .RELATIONAL_OPERATOR_GREATER_THAN)
                                                .setRhsExpression(
                                                    Expression.newBuilder()
                                                        .setValue(
                                                            Value.newBuilder()
                                                                .setStringValue("v1"))))))
                        .build()));

    assertEquals(
        "INVALID_ARGUMENT: Array filter operator is mandatory",
        statusRuntimeException.getMessage());
  }

  @Test
  void testValidArrayFilterConditionWithMultipleValues() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_STRING_ARRAY).build()));

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
                    .setFilterCriteria(
                        FilterCriteria.newBuilder()
                            .setArrayFilter(
                                ArrayFilterCondition.newBuilder()
                                    .setOperator(ARRAY_OPERATOR_ALL)
                                    .setRelationalFilter(
                                        RelationalFilterCondition.newBuilder()
                                            .setLhsExpression(
                                                Expression.newBuilder()
                                                    .setField(
                                                        Field.newBuilder().setKey("validKey")))
                                            .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                                            .setRhsExpression(
                                                Expression.newBuilder()
                                                    .setValue(
                                                        Value.newBuilder()
                                                            .setListValue(
                                                                ListValue.newBuilder()
                                                                    .addValues(
                                                                        Value.newBuilder()
                                                                            .setStringValue(
                                                                                "v1"))))))))
                    .build()));
  }

  @Test
  void testFilterConditionWithValidAttributeAndInvalidOperator() {
    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockAttributeServiceCachedClient.get(mockRequestContext, "traces", "validKey"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setValueKind(TYPE_BOOL).build()));

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
                        .setFilterCriteria(
                            FilterCriteria.newBuilder()
                                .setRelationalFilter(
                                    RelationalFilterCondition.newBuilder()
                                        .setLhsExpression(
                                            Expression.newBuilder()
                                                .setField(Field.newBuilder().setKey("validKey"))
                                                .build())
                                        .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                                        .setRhsExpression(
                                            Expression.newBuilder()
                                                .setValue(
                                                    Value.newBuilder()
                                                        .setListValue(
                                                            ListValue.newBuilder()
                                                                .addValues(
                                                                    Value.newBuilder()
                                                                        .setBoolValue(true)
                                                                        .build())
                                                                .build()))
                                                .build())
                                        .build()))
                        .build()));

    assertEquals(
        "INVALID_ARGUMENT: Unsupported operator RELATIONAL_OPERATOR_IN for attributeKind TYPE_BOOL",
        statusRuntimeException.getMessage());
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
  }

  @Test
  void testFilterConditionForUpdateRequest() {

    when(mockRequestContext.getUserId()).thenReturn(Optional.of(TEST_USER_ID));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(mockSavedFilterStoreManager.fetchSavedFilters(
            mockRequestContext, GetSavedFiltersRequest.newBuilder().setId("id-1").build()))
        .thenReturn(
            GetSavedFiltersResponse.newBuilder()
                .addSavedFilters(SavedFilter.newBuilder().setScope("scope").build())
                .build());

    StatusRuntimeException statusRuntimeException =
        assertThrows(
            StatusRuntimeException.class,
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
    assertEquals(
        "UNIMPLEMENTED: Filter type FILTERCONDITION_NOT_SET is not supported",
        statusRuntimeException.getMessage());
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
