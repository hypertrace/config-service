package ai.traceable.attribute.resolution.config.service.v1.validation;

import static ai.traceable.attribute.resolution.config.service.v1.KeyLocation.KEY_LOCATION_REQUEST_HEADER;
import static ai.traceable.attribute.resolution.config.service.v1.KeyLocation.KEY_LOCATION_URL_PATH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_ENDS_WITH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EQUALS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EXISTS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_IN;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_CONTAINS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_EQUALS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.attribute.resolution.config.service.v1.Action;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigData;
import ai.traceable.attribute.resolution.config.service.v1.ConfigScope;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.CustomerScope;
import ai.traceable.attribute.resolution.config.service.v1.DeleteAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.DynamicAction;
import ai.traceable.attribute.resolution.config.service.v1.EnvironmentScope;
import ai.traceable.attribute.resolution.config.service.v1.Filter;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsRequest;
import ai.traceable.attribute.resolution.config.service.v1.KeyFilter;
import ai.traceable.attribute.resolution.config.service.v1.LogicalFilter;
import ai.traceable.attribute.resolution.config.service.v1.LogicalOperator;
import ai.traceable.attribute.resolution.config.service.v1.MatchGroupOperation;
import ai.traceable.attribute.resolution.config.service.v1.NoOperation;
import ai.traceable.attribute.resolution.config.service.v1.RelationalFilter;
import ai.traceable.attribute.resolution.config.service.v1.RelationalOperator;
import ai.traceable.attribute.resolution.config.service.v1.ScopedCondition;
import ai.traceable.attribute.resolution.config.service.v1.ServiceScope;
import ai.traceable.attribute.resolution.config.service.v1.StaticAction;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.ValueFilter;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AttributeResolutionConfigValidatorImplTest {

  private AttributeResolutionConfigValidatorImpl validator;

  @Mock private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new AttributeResolutionConfigValidatorImpl();
    when(requestContext.getTenantId()).thenReturn(java.util.Optional.of("test-tenant"));
  }

  @Test
  void testValidateGetAttributeResolutionConfigsRequest_Valid() {
    GetAttributeResolutionConfigsRequest request =
        GetAttributeResolutionConfigsRequest.newBuilder().setFilter(createValidGetFilter()).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateGetAttributeResolutionConfigsRequest_InvalidContext() {
    when(requestContext.getTenantId()).thenReturn(java.util.Optional.empty());
    GetAttributeResolutionConfigsRequest request =
        GetAttributeResolutionConfigsRequest.newBuilder().setFilter(createValidGetFilter()).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateAttributeResolutionConfigRequest_Valid() {
    CreateAttributeResolutionConfigRequest request = createValidCreateRequest();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateAttributeResolutionConfigRequest_BlankName() {
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder().setName("").build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateAttributeResolutionConfigRequest_BlankAttributeKey() {
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder().setAttributeKey("").build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateAttributeResolutionConfigRequest_BlankEntityType() {
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder().setEntityType("").build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateAttributeResolutionConfigRequest_EmptyScopedConditionList() {
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder().clearScopedConditions().build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateUpdateAttributeResolutionConfigRequest_Valid() {
    UpdateAttributeResolutionConfigRequest request =
        UpdateAttributeResolutionConfigRequest.newBuilder()
            .setConfig(createValidAttributeResolutionConfig())
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateUpdateAttributeResolutionConfigRequest_BlankId() {
    AttributeResolutionConfig config =
        createValidAttributeResolutionConfig().toBuilder().setId("").build();
    UpdateAttributeResolutionConfigRequest request =
        UpdateAttributeResolutionConfigRequest.newBuilder().setConfig(config).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateDeleteAttributeResolutionConfigRequest_Valid() {
    DeleteAttributeResolutionConfigRequest request =
        DeleteAttributeResolutionConfigRequest.newBuilder().setId("config-123").build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateDeleteAttributeResolutionConfigRequest_MissingId() {
    DeleteAttributeResolutionConfigRequest request =
        DeleteAttributeResolutionConfigRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateAction_StaticAction_Valid() {
    Action action = Action.newBuilder().setStaticAction(createValidStaticAction()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setAction(action).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateAction_DynamicAction_Valid() {
    Action action = Action.newBuilder().setDynamicAction(createValidDynamicAction()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setAction(action).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateAction_InvalidActionCase() {
    Action action = Action.newBuilder().build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setAction(action).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateFilter_RelationalFilter_Valid() {
    Filter filter = Filter.newBuilder().setRelationalFilter(createValidRelationalFilter()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateFilter_LogicalFilter_Valid() {
    Filter filter = Filter.newBuilder().setLogicalFilter(createValidLogicalFilter()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateFilter_InvalidFilterCase() {
    Filter filter = Filter.newBuilder().build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateScope_CustomerScope_Valid() {
    CustomerScope customerScope = CustomerScope.newBuilder().build();
    ConfigScope scope = ConfigScope.newBuilder().setCustomerScope(customerScope).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateScope_EnvironmentScope_Valid() {
    ConfigScope scope =
        ConfigScope.newBuilder().setEnvironmentScope(createValidEnvironmentScope()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateScope_ServiceScope_Valid() {
    ConfigScope scope = ConfigScope.newBuilder().setServiceScope(createValidServiceScope()).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateScope_InvalidScopeCase() {
    ConfigScope scope = ConfigScope.newBuilder().build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateEnvironmentScope_EmptyEnvironmentIds() {
    EnvironmentScope environmentScope = EnvironmentScope.newBuilder().build();
    ConfigScope scope = ConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateServiceScope_EmptyServiceIds() {
    ServiceScope serviceScope = ServiceScope.newBuilder().build();
    ConfigScope scope = ConfigScope.newBuilder().setServiceScope(serviceScope).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateServiceScope_WithEnvironmentScope_Valid() {
    ServiceScope serviceScope =
        ServiceScope.newBuilder()
            .addServiceIds("service-123")
            .setEnvironmentScope(createValidEnvironmentScope())
            .build();
    ConfigScope scope = ConfigScope.newBuilder().setServiceScope(serviceScope).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setScope(scope).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @ParameterizedTest(name = "testValidateDynamicAction_{0}")
  @MethodSource("provideDynamicActionTestCases")
  void testValidateDynamicAction(String testName, String matchGroup, String matchGroupRegex) {
    MatchGroupOperation matchGroupOperation =
        MatchGroupOperation.newBuilder()
            .setMatchGroup(matchGroup)
            .setMatchGroupRegex(matchGroupRegex)
            .build();
    DynamicAction dynamicAction =
        createValidDynamicAction().toBuilder().setMatchGroupOperation(matchGroupOperation).build();
    Action action = Action.newBuilder().setDynamicAction(dynamicAction).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setAction(action).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();

    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  private static Stream<Arguments> provideDynamicActionTestCases() {
    return Stream.of(
        Arguments.of("BlankMatchGroup", "", "test-regex"),
        Arguments.of("BlankMatchGroupRegex", "group1", ""),
        Arguments.of("InvalidRegex", "group1", "[invalid"));
  }

  private CreateAttributeResolutionConfigRequest createValidCreateRequest() {
    return CreateAttributeResolutionConfigRequest.newBuilder()
        .setData(createValidAttributeResolutionConfigData())
        .build();
  }

  private AttributeResolutionConfig createValidAttributeResolutionConfig() {
    return AttributeResolutionConfig.newBuilder()
        .setId("config-123")
        .setData(createValidAttributeResolutionConfigData())
        .build();
  }

  private AttributeResolutionConfigData createValidAttributeResolutionConfigData() {
    return AttributeResolutionConfigData.newBuilder()
        .setName("Test Config")
        .setAttributeKey("test.attribute")
        .setEntityType("SERVICE")
        .addScopedConditions(createValidScopedCondition())
        .build();
  }

  private ScopedCondition createValidScopedCondition() {
    return ScopedCondition.newBuilder()
        .setScope(createValidScope())
        .setFilter(createValidFilter())
        .setAction(createValidAction())
        .build();
  }

  private ConfigScope createValidScope() {
    CustomerScope customerScope = CustomerScope.newBuilder().build();
    return ConfigScope.newBuilder().setCustomerScope(customerScope).build();
  }

  private Filter createValidFilter() {
    return Filter.newBuilder().setRelationalFilter(createValidRelationalFilter()).build();
  }

  private RelationalFilter createValidRelationalFilter() {
    return RelationalFilter.newBuilder()
        .setKeyFilter(createValidKeyFilter())
        .setValueFilter(createValidValueFilter())
        .build();
  }

  private LogicalFilter createValidLogicalFilter() {
    return LogicalFilter.newBuilder()
        .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
        .addFilters(createValidFilter())
        .build();
  }

  private KeyFilter createValidKeyFilter() {
    return KeyFilter.newBuilder()
        .setLocation(KEY_LOCATION_REQUEST_HEADER)
        .setOperator(RELATIONAL_OPERATOR_EQUALS)
        .setValue(Value.newBuilder().setStringValue("test-key"))
        .build();
  }

  private ValueFilter createValidValueFilter() {
    return ValueFilter.newBuilder()
        .setOperator(RELATIONAL_OPERATOR_EQUALS)
        .setValue(Value.newBuilder().setStringValue("test-value"))
        .build();
  }

  private Action createValidAction() {
    return Action.newBuilder().setStaticAction(createValidStaticAction()).build();
  }

  private StaticAction createValidStaticAction() {
    return StaticAction.newBuilder()
        .setValue(Value.newBuilder().setStringValue("static-value"))
        .build();
  }

  private DynamicAction createValidDynamicAction() {
    return DynamicAction.newBuilder()
        .setKeyFilter(createValidKeyFilter())
        .setNoOperation(NoOperation.newBuilder().build())
        .build();
  }

  private GetAttributeResolutionConfigsFilter createValidGetFilter() {
    return GetAttributeResolutionConfigsFilter.newBuilder().setEntityType("SERVICE").build();
  }

  private EnvironmentScope createValidEnvironmentScope() {
    return EnvironmentScope.newBuilder().addEnvironmentIds("env-123").build();
  }

  private ServiceScope createValidServiceScope() {
    return ServiceScope.newBuilder().addServiceIds("service-123").build();
  }

  @ParameterizedTest(name = "testValidateKeyFilter_UrlPath_ValidOperator_{0}")
  @MethodSource("provideValidUrlPathOperators")
  void testValidateKeyFilter_UrlPath_ValidOperators(String testName, RelationalOperator operator) {
    KeyFilter keyFilter = createUrlPathKeyFilter(operator);
    RelationalFilter relationalFilter =
        RelationalFilter.newBuilder().setKeyFilter(keyFilter).build();
    Filter filter = Filter.newBuilder().setRelationalFilter(relationalFilter).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @ParameterizedTest(name = "testValidateKeyFilter_UrlPath_InvalidOperator_{0}")
  @MethodSource("provideInvalidUrlPathOperators")
  void testValidateKeyFilter_UrlPath_InvalidOperators(
      String testName, RelationalOperator operator) {
    KeyFilter keyFilter = createUrlPathKeyFilter(operator);
    RelationalFilter relationalFilter =
        RelationalFilter.newBuilder().setKeyFilter(keyFilter).build();
    Filter filter = Filter.newBuilder().setRelationalFilter(relationalFilter).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();

    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateKeyFilter_UrlPath_NoValueFilter() {
    // URL path location should not require a value filter
    KeyFilter keyFilter = createUrlPathKeyFilter(RELATIONAL_OPERATOR_EQUALS);
    RelationalFilter relationalFilter =
        RelationalFilter.newBuilder()
            .setKeyFilter(keyFilter)
            // No value filter set for URL path
            .build();
    Filter filter = Filter.newBuilder().setRelationalFilter(relationalFilter).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateKeyFilter_NonUrlPath_RequiresValueFilter() {
    // Non-URL path locations should require a value filter
    KeyFilter keyFilter =
        KeyFilter.newBuilder()
            .setLocation(KEY_LOCATION_REQUEST_HEADER)
            .setOperator(RELATIONAL_OPERATOR_EQUALS)
            .setValue(Value.newBuilder().setStringValue("/api/test"))
            .build();
    RelationalFilter relationalFilter =
        RelationalFilter.newBuilder()
            .setKeyFilter(keyFilter)
            // No value filter set for non-URL path location
            .build();
    Filter filter = Filter.newBuilder().setRelationalFilter(relationalFilter).build();
    ScopedCondition scopedCondition =
        createValidScopedCondition().toBuilder().setFilter(filter).build();
    AttributeResolutionConfigData data =
        createValidAttributeResolutionConfigData().toBuilder()
            .clearScopedConditions()
            .addScopedConditions(scopedCondition)
            .build();
    CreateAttributeResolutionConfigRequest request =
        CreateAttributeResolutionConfigRequest.newBuilder().setData(data).build();

    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  private static Stream<Arguments> provideValidUrlPathOperators() {
    return Stream.of(
        Arguments.of("Equals", RELATIONAL_OPERATOR_EQUALS),
        Arguments.of("NotEquals", RELATIONAL_OPERATOR_NOT_EQUALS),
        Arguments.of("Contains", RELATIONAL_OPERATOR_CONTAINS),
        Arguments.of("StartsWith", RELATIONAL_OPERATOR_STARTS_WITH),
        Arguments.of("EndsWith", RELATIONAL_OPERATOR_ENDS_WITH),
        Arguments.of("RegexMatch", RELATIONAL_OPERATOR_REGEX_MATCH),
        Arguments.of("NotContains", RELATIONAL_OPERATOR_NOT_CONTAINS));
  }

  private static Stream<Arguments> provideInvalidUrlPathOperators() {
    return Stream.of(
        Arguments.of("In", RELATIONAL_OPERATOR_IN),
        Arguments.of("Exists", RELATIONAL_OPERATOR_EXISTS));
  }

  private KeyFilter createUrlPathKeyFilter(RelationalOperator operator) {
    return KeyFilter.newBuilder()
        .setLocation(KEY_LOCATION_URL_PATH)
        .setOperator(operator)
        .setValue(Value.newBuilder().setStringValue("/api/test"))
        .build();
  }
}
