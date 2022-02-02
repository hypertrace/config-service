package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ApiScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class DataTypeConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;

  public DataTypeConfigRequestValidatorTest() {
    this.dataTypeConfigRequestValidator = new DataTypeConfigRequestValidator();
  }

  @Test
  void validateOrThrowNoDataTypeRule() {
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowNoName() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(createStringPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowEmptyScopedPattern() {
    DataTypeRule rule = DataTypeRule.newBuilder().setName("name-1").build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowNoScope() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(createStringPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowEmptyScope() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(ApiScope.newBuilder().build())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(createStringPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowNoLocation() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .setKeyPattern(createStringPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowUnspecifiedLocation() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .setKeyPattern(createStringPattern())
                    .addLocations(LOCATION_UNSPECIFIED)
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowNoKeyPattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowWrongKeyPattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(createStringPatternNoValue())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowCorrectKeyPattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(createStringPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowEmptyKeyPattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyPattern(StringPattern.newBuilder().build())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowWrongKeyValuePattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyValuePattern(createKeyValuePatternNoKeyPattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
        });
  }

  @Test
  void validateOrThrowCorrectKeyValuePattern() {
    DataTypeRule rule =
        DataTypeRule.newBuilder()
            .setName("name-1")
            .addScopedPatterns(
                ScopedPattern.newBuilder()
                    .setApiScope(createApiScope())
                    .addLocations(LOCATION_REQUEST_HEADER)
                    .setKeyValuePattern(createKeyValuePattern())
                    .setActionValue(1))
            .build();
    CreateDataTypeRequest request = CreateDataTypeRequest.newBuilder().setRule(rule).build();
    dataTypeConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  private ApiScope createApiScope() {
    return ApiScope.newBuilder().addAllApiIds(List.of("1", "2")).build();
  }

  private StringPattern createStringPattern() {
    return StringPattern.newBuilder()
        .setOperator(Operator.OPERATOR_EQUALS)
        .setValue("value-1")
        .build();
  }

  private StringPattern createStringPatternNoValue() {
    return StringPattern.newBuilder().setOperator(Operator.OPERATOR_EQUALS).build();
  }

  private KeyValuePattern createKeyValuePattern() {
    return KeyValuePattern.newBuilder()
        .setKeyPattern(createStringPattern())
        .setValuePattern(createStringPattern())
        .build();
  }

  private KeyValuePattern createKeyValuePatternNoKeyPattern() {
    return KeyValuePattern.newBuilder().setValuePattern(createStringPattern()).build();
  }
}
