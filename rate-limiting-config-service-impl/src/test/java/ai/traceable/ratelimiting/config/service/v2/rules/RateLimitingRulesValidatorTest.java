package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.Block;
import ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity;
import ai.traceable.ratelimiting.config.service.v2.ApiAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.EmailDomainCondition;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionType;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.StringCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition.Region;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig.RollingWindowThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import ai.traceable.ratelimiting.config.service.v2.UserAggregateType;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesValidator;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import jdk.jfr.Description;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class RateLimitingRulesValidatorTest {
  private RequestContext requestContext;
  private RateLimitingRulesValidator rulesValidator;

  @BeforeEach
  void setUp() {
    requestContext = RequestContext.forTenantId("default tenant");
    rulesValidator = new RateLimitingRulesValidator();
  }

  @Test
  void testCategoryNotSet() {
    RateLimitingRuleData ruleData = RateLimitingRuleData.getDefaultInstance();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    RateLimitingRuleData.getDescriptor()
                        .findFieldByNumber(RateLimitingRuleData.CATEGORY_FIELD_NUMBER))));
  }

  @Test
  @Description("Should return invalid argument on creating rule with duplicate name in same type")
  void validateCreateRateLimitingRule_same_name() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(false)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        ScopeCondition.EntityScope.newBuilder()
                                            .setEntityType(
                                                ScopeCondition.EntityType.ENTITY_TYPE_API)
                                            .addEntityIds("id1")
                                            .build())
                                    .build())
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue("^a")
                                            .build())
                                    .build()))
                    .build())
            .build();
    RateLimitingRule rule = RateLimitingRule.newBuilder().setId("id-1").setData(ruleData).build();
    RateLimitingRuleData ruleData1 =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue("^a")
                                            .build())
                                    .build()))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData1).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of(rule)));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  @Description(
      "Should return invalid argument on updating rule with duplicate name in same category")
  void validateUpdateRateLimitingRule_same_name() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue("^a")
                                            .build())
                                    .build()))
                    .build())
            .build();
    RateLimitingRule rule = RateLimitingRule.newBuilder().setId("id-1").setData(ruleData).build();
    RateLimitingRuleData ruleData1 =
        RateLimitingRuleData.newBuilder()
            .setName("rule2")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(false)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue("^a")
                                            .build())
                                    .build()))
                    .build())
            .build();
    RateLimitingRule rule1 = RateLimitingRule.newBuilder().setId("id-2").setData(ruleData1).build();
    UpdateRateLimitingRuleRequest request =
        UpdateRateLimitingRuleRequest.newBuilder().setRuleId("id-2").setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of(rule, rule1)));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testNoAction() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        CompositeCondition.newBuilder()
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                RegionCondition.newBuilder()
                                                    .addAllRegions(List.of("IND", "US"))))
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setEntityScope(
                                                        ScopeCondition.EntityScope.newBuilder()
                                                            .setEntityType(
                                                                ScopeCondition.EntityType
                                                                    .ENTITY_TYPE_API)
                                                            .addEntityIds("id1")
                                                            .build())
                                                    .build())
                                            .build()))
                            .build()))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected at least 1 value for repeated field %s but not present",
                    RateLimitingRuleData.getDescriptor()
                        .findFieldByNumber(
                            RateLimitingRuleData.THRESHOLD_ACTION_CONFIGS_FIELD_NUMBER))));
  }

  @Test
  void testRollingWindowDurationNotSet() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        CompositeCondition.newBuilder()
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                RegionCondition.newBuilder()
                                                    .addAllRegions(List.of("IND", "US"))))
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setEntityScope(
                                                        ScopeCondition.EntityScope.newBuilder()
                                                            .setEntityType(
                                                                ScopeCondition.EntityType
                                                                    .ENTITY_TYPE_API)
                                                            .addEntityIds("id1")
                                                            .build())
                                                    .build())
                                            .build()))
                            .build()))
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                    .newBuilder()
                                    .setCountAllowed(1000)
                                    .build())
                            .build()))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    ResourceAccessThresholdConfig.RollingWindowThresholdConfig.getDescriptor()
                        .findFieldByNumber(
                            ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                .DURATION_ISO_FIELD_NUMBER))));
  }

  @Test
  void testValueBasedThresholdConfigValidation() {
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithValueBasedCondition(
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.getDefaultInstance()))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.getDescriptor()
                        .findFieldByNumber(
                            ResourceAccessThresholdConfig.ValueBasedThresholdConfig
                                .UNIQUE_VALUES_ALLOWED_FIELD_NUMBER))));

    CreateRateLimitingRuleRequest request1 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithValueBasedCondition(
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.newBuilder()
                        .setUniqueValuesAllowed(10)
                        .build()))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request1, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.getDescriptor()
                        .findFieldByNumber(
                            ResourceAccessThresholdConfig.ValueBasedThresholdConfig
                                .DURATION_ISO_FIELD_NUMBER))));

    CreateRateLimitingRuleRequest request2 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithValueBasedCondition(
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.newBuilder()
                        .setUniqueValuesAllowed(10)
                        .setDurationIso("1h")
                        .build()))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request2, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.getDescriptor()
                        .findFieldByNumber(
                            ResourceAccessThresholdConfig.ValueBasedThresholdConfig
                                .VALUE_TYPE_FIELD_NUMBER))));

    CreateRateLimitingRuleRequest request3 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithValueBasedCondition(
                    ResourceAccessThresholdConfig.ValueBasedThresholdConfig.newBuilder()
                        .setUniqueValuesAllowed(10)
                        .setDurationIso("1h")
                        .setValueType(
                            ResourceAccessThresholdConfig.ValueType.VALUE_TYPE_SENSITIVE_PARAMS)
                        .build()))
            .build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request3, List.of()));
  }

  @Test
  void testInvalidRuleConfigScope_emptyEnvironmentScope() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        CompositeCondition.newBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpLocationTypeCondition(
                                                buildIpLocationTypeCondition(
                                                    List.of(
                                                        IpLocationType.IP_LOCATION_TYPE_ANONYMOUS,
                                                        IpLocationType
                                                            .IP_LOCATION_TYPE_RESIDENTIAL)))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                buildRegionCondition(List.of("IND", "US")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                buildDatatypeCondition(List.of("id1", "id2")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setEntityScope(
                                                        ScopeCondition.EntityScope.newBuilder()
                                                            .setEntityType(
                                                                ScopeCondition.EntityType
                                                                    .ENTITY_TYPE_API)
                                                            .addEntityIds("id1")
                                                            .build())
                                                    .build())
                                            .build()))
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND))
                    .build())
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                    .build())
                            .build())
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S")
                                    .build())
                            .build()))
            .setRuleConfigScope(
                RuleConfigScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder()))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected at least 1 value for repeated field %s but not present",
                    EnvironmentScope.getDescriptor()
                        .findFieldByNumber(EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER))));
  }

  @Test
  void testInvalidRuleConfigScope_emptyEnvironmentIds() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        CompositeCondition.newBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpLocationTypeCondition(
                                                buildIpLocationTypeCondition(
                                                    List.of(
                                                        IpLocationType.IP_LOCATION_TYPE_ANONYMOUS,
                                                        IpLocationType
                                                            .IP_LOCATION_TYPE_RESIDENTIAL)))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                buildRegionCondition(List.of("IND", "US")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                buildDatatypeCondition(List.of("id1", "id2")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setEntityScope(
                                                        ScopeCondition.EntityScope.newBuilder()
                                                            .setEntityType(
                                                                ScopeCondition.EntityType
                                                                    .ENTITY_TYPE_API)
                                                            .addEntityIds("id1")
                                                            .build())
                                                    .build())
                                            .build()))
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND))
                    .build())
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                    .build())
                            .build())
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S")
                                    .build())
                            .build()))
            .setRuleConfigScope(
                RuleConfigScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("")))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(status.getDescription(), "Environment id should not be empty string.");
  }

  @Test
  void testInvalidRule_invalidRegex() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_URL)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue("*")
                                            .build())
                                    .build())))
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                    .build())
                            .build())
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S")
                                    .build())
                            .build()))
            .setRuleConfigScope(RuleConfigScope.newBuilder())
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
  }

  @Test
  void testValidRule() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        CompositeCondition.newBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setKeyValueCondition(
                                                KeyValueCondition.newBuilder()
                                                    .setType(Type.TYPE_URL)
                                                    .setKeyCondition(
                                                        StringCondition.newBuilder()
                                                            .setOperator(
                                                                MatchOperator
                                                                    .MATCH_OPERATOR_MATCHES_REGEX)
                                                            .setValue("^a")
                                                            .build())
                                                    .build())))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpLocationTypeCondition(
                                                buildIpLocationTypeCondition(
                                                    List.of(
                                                        IpLocationType.IP_LOCATION_TYPE_ANONYMOUS,
                                                        IpLocationType
                                                            .IP_LOCATION_TYPE_RESIDENTIAL)))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                buildRegionCondition(List.of("IND", "US")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpAddressCondition(
                                                buildIpAddressCondition(
                                                    List.of("1.2.3.4/24", "127.0.0.1")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                buildDatatypeCondition(List.of("id1", "id2")))))
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setEntityScope(
                                                        ScopeCondition.EntityScope.newBuilder()
                                                            .setEntityType(
                                                                ScopeCondition.EntityType
                                                                    .ENTITY_TYPE_API)
                                                            .addEntityIds("id1")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setUserIdCondition(
                                                UserIdCondition.newBuilder()
                                                    .addActorEntityIds("userId")
                                                    .addUserIdRegexes("userId.*")
                                                    .build())
                                            .build())
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setEmailDomainCondition(
                                                EmailDomainCondition.newBuilder()
                                                    .addEmailDomains("@abc")
                                                    .addEmailRegexes("@foo.*")
                                                    .build())
                                            .build())
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setUserAgentCondition(
                                                UserAgentCondition.newBuilder()
                                                    .addUserAgents("agent")
                                                    .addUserAgentRegexes("userAgent.*")
                                                    .build())
                                            .build())
                                    .build())
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpConnectionTypeCondition(
                                                IpConnectionTypeCondition.newBuilder()
                                                    .addIpConnectionTypes(
                                                        IpConnectionType
                                                            .IP_CONNECTION_TYPE_CORPORATE)
                                                    .build())
                                            .build())
                                    .build())
                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND))
                    .build())
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                    .build())
                            .build())
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S")
                                    .build())
                            .build()))
            .setRuleConfigScope(RuleConfigScope.newBuilder())
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
  }

  @Test
  void testDynamicThresholdConfigValidation() {
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.getDefaultInstance(),
                    ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT,
                    UserAggregateType.USER_AGGREGATE_TYPE_PER_USER))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        Objects.requireNonNull(status.getDescription())
            .contains(
                String.format(
                    "Expected field value %s but not present",
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.getDescriptor()
                        .findFieldByNumber(
                            ResourceAccessThresholdConfig.DynamicThresholdConfig
                                .PERCENT_EXCEEDING_MEAN_ALLOWED_FIELD_NUMBER))));

    CreateRateLimitingRuleRequest request1 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT,
                    UserAggregateType.USER_AGGREGATE_TYPE_PER_USER))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request1, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Dynamic threshold config does not have valid mean calculation duration",
        status.getDescription());

    CreateRateLimitingRuleRequest request2 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .setMeanCalculationDuration(Duration.newBuilder().setSeconds(10000).build())
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT,
                    UserAggregateType.USER_AGGREGATE_TYPE_PER_USER))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request2, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Dynamic threshold config does not have valid duration", status.getDescription());

    CreateRateLimitingRuleRequest request3 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .setMeanCalculationDuration(Duration.newBuilder().setSeconds(10000).build())
                        .setDuration(Duration.newBuilder().setSeconds(10000).build())
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS,
                    UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request3, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Dynamic threshold supports aggregation only on per user and per api endpoint",
        status.getDescription());

    CreateRateLimitingRuleRequest request4 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .setMeanCalculationDuration(Duration.newBuilder().setSeconds(10000).build())
                        .setDuration(Duration.newBuilder().setSeconds(10000).build())
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT,
                    UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request4, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Dynamic threshold supports aggregation only on per user and per api endpoint",
        status.getDescription());

    CreateRateLimitingRuleRequest request5 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .setMeanCalculationDuration(Duration.newBuilder().setSeconds(10000).build())
                        .setDuration(Duration.newBuilder().setSeconds(10000).build())
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS,
                    UserAggregateType.USER_AGGREGATE_TYPE_PER_USER))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request5, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Dynamic threshold supports aggregation only on per user and per api endpoint",
        status.getDescription());

    CreateRateLimitingRuleRequest request6 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                getRateLimitingRuleDataWithDynamicThresholdCondition(
                    ResourceAccessThresholdConfig.DynamicThresholdConfig.newBuilder()
                        .setPercentExceedingMeanAllowed(200)
                        .setMeanCalculationDuration(Duration.newBuilder().setSeconds(10000).build())
                        .setDuration(Duration.newBuilder().setSeconds(10000).build())
                        .build(),
                    ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT,
                    UserAggregateType.USER_AGGREGATE_TYPE_PER_USER))
            .build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request6, List.of()));
  }

  @Nested
  class testRegionCondition {
    @Test
    void invalidRule_NoRegionSet() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(RegionCondition.newBuilder())))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertThrows(
          RuntimeException.class,
          () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    @Test
    void invalidRule_EmptyRegionsDeprecatedFlow() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(RegionCondition.newBuilder().addRegions(""))))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertThrows(
          RuntimeException.class,
          () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    @Test
    void validRule_DeprecatedFlow() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(
                                  RegionCondition.newBuilder().addRegions("efefef"))))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    @Test
    void invalidRule_EmptyRegions() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(
                                  RegionCondition.newBuilder()
                                      .addRegionIdentifiers(Region.getDefaultInstance()))))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertThrows(
          RuntimeException.class,
          () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    @Test
    void invalidRule_EmptyRegions2() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(
                                  RegionCondition.newBuilder()
                                      .addRegionIdentifiers(
                                          Region.newBuilder().setCountryIsoCode("")))))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertThrows(
          RuntimeException.class,
          () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    @Test
    void validRule() {
      RateLimitingRuleData ruleData =
          RateLimitingRuleData.newBuilder()
              .setName("rule1")
              .setCategory(Category.CATEGORY_RATE_LIMITING)
              .setEnabled(true)
              .setCondition(
                  Condition.newBuilder()
                      .setLeafCondition(
                          LeafCondition.newBuilder()
                              .setRegionCondition(
                                  RegionCondition.newBuilder()
                                      .addRegionIdentifiers(
                                          Region.newBuilder().setCountryIsoCode("ssfsd")))))
              .addThresholdActionConfigs(
                  ThresholdActionConfig.newBuilder()
                      .addActions(
                          Action.newBuilder()
                              .setBlock(
                                  Block.newBuilder()
                                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                      .build())
                              .build())
                      .addResourceAccessThresholdConfigs(
                          ResourceAccessThresholdConfig.newBuilder()
                              .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                              .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                              .setRollingWindowThresholdConfig(
                                  RollingWindowThresholdConfig.newBuilder()
                                      .setCountAllowed(1000)
                                      .setDurationIso("P3Y6M4DT12H30M5S")
                                      .build())
                              .build()))
              .setRuleConfigScope(RuleConfigScope.newBuilder())
              .build();
      CreateRateLimitingRuleRequest request =
          CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
      assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    }
  }

  @Test
  void testUserAggregateAcrossAll() {
    // aggregation across all users not supported for block
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setRegionCondition(
                                RegionCondition.newBuilder()
                                    .addRegionIdentifiers(
                                        Region.newBuilder().setCountryIsoCode("ssfsd")))))
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                                    .build())
                            .build())
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(
                                UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S")
                                    .build())
                            .build()))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals("Block action unsupported on aggregation across users", status.getDescription());

    // aggregation across all users supported for alert
    ThresholdActionConfig thresholdActionConfig =
        ruleData.getThresholdActionConfigsList().get(0).toBuilder()
            .clearActions()
            .addActions(Action.newBuilder().setAlert(Action.Alert.newBuilder()))
            .build();
    ruleData =
        ruleData.toBuilder()
            .clearThresholdActionConfigs()
            .addThresholdActionConfigs(thresholdActionConfig)
            .build();
    CreateRateLimitingRuleRequest request1 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request1, List.of()));
  }

  private RegionCondition buildRegionCondition(List<String> regions) {
    return RegionCondition.newBuilder().addAllRegions(regions).build();
  }

  private DatatypeCondition buildDatatypeCondition(List<String> datasetIds) {
    return DatatypeCondition.newBuilder().addAllDatasetIds(datasetIds).build();
  }

  private IpLocationTypeCondition buildIpLocationTypeCondition(
      List<IpLocationType> ipLocationTypes) {
    return IpLocationTypeCondition.newBuilder().addAllIpLocationTypes(ipLocationTypes).build();
  }

  private IpAddressCondition buildIpAddressCondition(List<String> rawIpList) {
    return IpAddressCondition.newBuilder().addAllRawInputIpData(rawIpList).build();
  }

  private RateLimitingRuleData getRateLimitingRuleDataWithValueBasedCondition(
      ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig) {
    return RateLimitingRuleData.newBuilder()
        .setName("rule1")
        .setCategory(Category.CATEGORY_RATE_LIMITING)
        .setEnabled(true)
        .setCondition(
            Condition.newBuilder()
                .setCompositeCondition(
                    CompositeCondition.newBuilder()
                        .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                        .addChildren(
                            Condition.newBuilder()
                                .setLeafCondition(
                                    LeafCondition.newBuilder()
                                        .setRegionCondition(
                                            RegionCondition.newBuilder()
                                                .addAllRegions(List.of("IND", "US"))))
                                .build())
                        .addChildren(
                            Condition.newBuilder()
                                .setLeafCondition(
                                    LeafCondition.newBuilder()
                                        .setScopeCondition(
                                            ScopeCondition.newBuilder()
                                                .setEntityScope(
                                                    ScopeCondition.EntityScope.newBuilder()
                                                        .setEntityType(
                                                            ScopeCondition.EntityType
                                                                .ENTITY_TYPE_API)
                                                        .addEntityIds("id1")
                                                        .build())
                                                .build())
                                        .build()))
                        .build()))
        .addThresholdActionConfigs(
            ThresholdActionConfig.newBuilder()
                .addActions(
                    Action.newBuilder()
                        .setAlert(
                            Action.Alert.newBuilder()
                                .setEventSeverity(Action.EventSeverity.EVENT_SEVERITY_HIGH)
                                .build())
                        .build())
                .addResourceAccessThresholdConfigs(
                    ResourceAccessThresholdConfig.newBuilder()
                        .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                        .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                        .setValueBasedThresholdConfig(valueBasedThresholdConfig)
                        .build()))
        .build();
  }

  private RateLimitingRuleData getRateLimitingRuleDataWithDynamicThresholdCondition(
      ResourceAccessThresholdConfig.DynamicThresholdConfig dynamicThresholdConfig,
      ApiAggregateType apiAggregateType,
      UserAggregateType userAggregateType) {
    return RateLimitingRuleData.newBuilder()
        .setName("rule1")
        .setCategory(Category.CATEGORY_RATE_LIMITING)
        .setEnabled(true)
        .setCondition(
            Condition.newBuilder()
                .setCompositeCondition(
                    CompositeCondition.newBuilder()
                        .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                        .addChildren(
                            Condition.newBuilder()
                                .setLeafCondition(
                                    LeafCondition.newBuilder()
                                        .setRegionCondition(
                                            RegionCondition.newBuilder()
                                                .addAllRegions(List.of("IND", "US"))))
                                .build())
                        .addChildren(
                            Condition.newBuilder()
                                .setLeafCondition(
                                    LeafCondition.newBuilder()
                                        .setScopeCondition(
                                            ScopeCondition.newBuilder()
                                                .setEntityScope(
                                                    ScopeCondition.EntityScope.newBuilder()
                                                        .setEntityType(
                                                            ScopeCondition.EntityType
                                                                .ENTITY_TYPE_API)
                                                        .addEntityIds("id1")
                                                        .build())
                                                .build())
                                        .build()))
                        .build()))
        .addThresholdActionConfigs(
            ThresholdActionConfig.newBuilder()
                .addActions(
                    Action.newBuilder()
                        .setAlert(
                            Action.Alert.newBuilder()
                                .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                                .build())
                        .build())
                .addResourceAccessThresholdConfigs(
                    ResourceAccessThresholdConfig.newBuilder()
                        .setApiAggregateType(apiAggregateType)
                        .setUserAggregateType(userAggregateType)
                        .setDynamicThresholdConfig(dynamicThresholdConfig)
                        .build()))
        .build();
  }
}
