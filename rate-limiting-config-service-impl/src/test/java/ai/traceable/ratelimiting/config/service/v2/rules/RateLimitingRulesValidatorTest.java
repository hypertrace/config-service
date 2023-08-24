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
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DataSensitivityLevel;
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
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
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
  @Description("Test for validating rate limiting data when transaction action config is present")
  void validateRateLimitingRuleData_for_transaction_action_config() {

    CompositeCondition compositeCondition =
        CompositeCondition.newBuilder()
            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
            .addChildren(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        ScopeCondition.UrlScope.newBuilder()
                                            .addAllUrlRegexes(List.of("^a"))
                                            .build())
                                    .build())
                            .build()))
            .build();

    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
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
                                                                    .ENTITY_TYPE_SERVICE)
                                                            .addAllEntityIds(
                                                                List.of("id1", "id2")))))))
                    .build())
            .setRuleConfigScope(RuleConfigScope.newBuilder())
            .setTransactionActionConfig(
                TransactionActionConfig.newBuilder()
                    .setAction(
                        Action.newBuilder()
                            .setAllow(Action.Allow.newBuilder().setDurationIso("iso").build())))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request, List.of()));

    RateLimitingRuleData ruleData1 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        ScopeCondition.EntityScope.newBuilder()
                                            .setEntityType(
                                                ScopeCondition.EntityType.ENTITY_TYPE_UNSPECIFIED)
                                            .addAllEntityIds(List.of("id1", "id2"))))))
            .build();
    CreateRateLimitingRuleRequest request1 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData1).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request1, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData2 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(DatatypeCondition.newBuilder()))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request2 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData2).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request2, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData3 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder()
                                    .addAllDatatypeIds(List.of("datatype1"))
                                    .setDataLocation(DataLocation.DATA_LOCATION_REQUEST)
                                    .addAllDataSensitivityLevels(
                                        List.of(DataSensitivityLevel.DATA_SENSITIVITY_LEVEL_LOW))
                                    .setDatatypeMatching(
                                        DatatypeCondition.DatatypeMatching.newBuilder()
                                            .setRegexBasedMatching(
                                                DatatypeCondition.RegexBasedMatching.newBuilder()
                                                    .setCustomMatchingLocation(
                                                        KeyValueCondition.newBuilder()
                                                            .setType(
                                                                Type.TYPE_REQUEST_BODY_PARAMETER)
                                                            .setKeyCondition(
                                                                StringCondition.newBuilder()
                                                                    .setOperator(
                                                                        MatchOperator
                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                    .setValue("^a"))))))))
            .build();
    CreateRateLimitingRuleRequest request3 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData3).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request3, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    RateLimitingRuleData ruleData4 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setEmailDomainCondition(
                                                EmailDomainCondition.newBuilder()
                                                    .setExclude(true)
                                                    .addAllEmailRegexes(List.of("e1", "e2"))
                                                    .addAllEmailDomains(List.of("r1", "r2"))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request4 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData4).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request4, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData5 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(RegionCondition.newBuilder()))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request5 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData5).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request5, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData6 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setKeyValueCondition(
                                                KeyValueCondition.newBuilder()
                                                    .setType(Type.TYPE_REQUEST_BODY_PARAMETER)))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request6 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData6).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request6, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData7 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpLocationTypeCondition(
                                                IpLocationTypeCondition.newBuilder()
                                                    .getDefaultInstanceForType()))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request7 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData7).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request7, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData8 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpAddressCondition(
                                                IpAddressCondition.newBuilder()))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request8 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData8).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request8, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData9 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY_PARAMETER)
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue(
                                                                                        "^a")))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request9 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData9).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request9, List.of()));

    RateLimitingRuleData ruleData10 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_RESPONSE)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY_PARAMETER)
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue(
                                                                                        "^a")))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request10 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData10).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request10, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData11 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY_PARAMETER)
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue(
                                                                                        "^a")))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request11 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData11).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request11, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData12 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setUrlScope(
                                                        ScopeCondition.UrlScope.newBuilder())))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request12 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData12).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request12, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData13 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setUrlScope(
                                                        ScopeCondition.UrlScope.newBuilder()
                                                            .addAllUrlRegexes(List.of("^a")))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request13 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData13).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request13, List.of()));

    RateLimitingRuleData ruleData14 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setScopeCondition(
                                                ScopeCondition.newBuilder()
                                                    .setLabelScope(
                                                        ScopeCondition.LabelScope.newBuilder()
                                                            .setLabelType(
                                                                ScopeCondition.LabelType
                                                                    .LABEL_TYPE_SERVICE)
                                                            .addAllLabelIds(List.of("lb1", "lb2"))
                                                            .build())))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request14 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData14).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request14, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData15 =
        ruleData.toBuilder()
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setCondition(Condition.newBuilder().setCompositeCondition(compositeCondition).build())
            .setTransactionActionConfig(TransactionActionConfig.getDefaultInstance())
            .build();
    CreateRateLimitingRuleRequest request15 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData15).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request15, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData16 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setIpAddressCondition(
                                                IpAddressCondition.newBuilder()
                                                    .setExclude(true)
                                                    .addAllRawInputIpData(List.of("1.2.3.4"))
                                                    .build())
                                            .build())))
                    .build())
            .setTransactionActionConfig(
                TransactionActionConfig.newBuilder()
                    .setAction(
                        Action.newBuilder()
                            .setAllow(Action.Allow.newBuilder().setDurationIso("iso").build())))
            .build();

    CreateRateLimitingRuleRequest request16 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData16).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request16, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData17 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setRegionCondition(
                                                RegionCondition.newBuilder()
                                                    .setExclude(true)
                                                    .addAllRegionIdentifiers(
                                                        List.of(
                                                            Region.newBuilder()
                                                                .setCountryIsoCode("IN")
                                                                .build()))
                                                    .build())
                                            .build())))
                    .build())
            .setTransactionActionConfig(
                TransactionActionConfig.newBuilder()
                    .setAction(
                        Action.newBuilder()
                            .setAllow(Action.Allow.newBuilder().setDurationIso("iso").build())))
            .build();
    CreateRateLimitingRuleRequest request17 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData17).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request17, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData18 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY_PARAMETER)
                                                                            .setValueCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue("^a"))
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue(
                                                                                        "^a")))))))))
                    .build())
            .build();

    CreateRateLimitingRuleRequest request18 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData18).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request18, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData19 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_RESPONSE_BODY)
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                                                    .setValue(
                                                                                        "^a")))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request19 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData19).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request19, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData20 =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
            .setEnabled(true)
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
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
                                                                    .ENTITY_TYPE_SERVICE)
                                                            .addAllEntityIds(
                                                                List.of("id1", "id2")))))))
                    .build())
            .setRuleConfigScope(RuleConfigScope.newBuilder())
            .setTransactionActionConfig(TransactionActionConfig.newBuilder())
            .build();
    CreateRateLimitingRuleRequest request20 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData20).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request20, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    RateLimitingRuleData ruleData21 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request21 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData21).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request21, List.of()));

    RateLimitingRuleData ruleData22 =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setCompositeCondition(
                        compositeCondition.toBuilder()
                            .addChildren(
                                Condition.newBuilder()
                                    .setLeafCondition(
                                        LeafCondition.newBuilder()
                                            .setDatatypeCondition(
                                                DatatypeCondition.newBuilder()
                                                    .addAllDatatypeIds(List.of("datatype1"))
                                                    .setDataLocation(
                                                        DataLocation.DATA_LOCATION_REQUEST)
                                                    .setDatatypeMatching(
                                                        DatatypeCondition.DatatypeMatching
                                                            .newBuilder()
                                                            .setRegexBasedMatching(
                                                                DatatypeCondition.RegexBasedMatching
                                                                    .newBuilder()
                                                                    .setCustomMatchingLocation(
                                                                        KeyValueCondition
                                                                            .newBuilder()
                                                                            .setType(
                                                                                Type
                                                                                    .TYPE_REQUEST_BODY)
                                                                            .setKeyCondition(
                                                                                StringCondition
                                                                                    .newBuilder()
                                                                                    .setValue("^a")
                                                                                    .setOperator(
                                                                                        MatchOperator
                                                                                            .MATCH_OPERATOR_MATCHES_REGEX)))))))))
                    .build())
            .build();
    CreateRateLimitingRuleRequest request22 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData22).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request22, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  @Description("Test for validation key value condition for rules other than dlp")
  void testKeyValueConditionForNonDlpRules() {
    RateLimitingRuleData ruleData =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Block.newBuilder()
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)))
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setApiAggregateType(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)
                            .setUserAggregateType(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
                            .setRollingWindowThresholdConfig(
                                RollingWindowThresholdConfig.newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("P3Y6M4DT12H30M5S"))))
            .setRuleConfigScope(RuleConfigScope.newBuilder())
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setValueCondition(
                                        StringCondition.newBuilder()
                                            .setValue("^a")
                                            .setOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)))))
            .build();
    CreateRateLimitingRuleRequest request =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    assertDoesNotThrow(() -> rulesValidator.validateOrThrow(requestContext, request, List.of()));

    ruleData =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setValue("^a")
                                            .setOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX))
                                    .setValueCondition(
                                        StringCondition.newBuilder()
                                            .setValue("^b")
                                            .setOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)))))
            .build();
    CreateRateLimitingRuleRequest request1 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request1, List.of()));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    ruleData =
        ruleData.toBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setKeyValueCondition(
                                KeyValueCondition.newBuilder()
                                    .setType(Type.TYPE_REQUEST_BODY)
                                    .setKeyCondition(
                                        StringCondition.newBuilder()
                                            .setValue("^b")
                                            .setOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)))))
            .build();
    CreateRateLimitingRuleRequest request2 =
        CreateRateLimitingRuleRequest.newBuilder().setData(ruleData).build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request2, List.of()));
    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
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
  void validateUpdateRateLimitingRule_cannot_update_rule_creation_source() {
    RateLimitingRuleData ruleData1 =
        RateLimitingRuleData.newBuilder()
            .setName("rule1")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(true)
            .setRuleStatus(
                RuleStatus.newBuilder()
                    .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_DEFAULT)
                    .setHidden(true)
                    .setGenerateInternalEvents(false)
                    .build())
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
    RateLimitingRule rule1 = RateLimitingRule.newBuilder().setId("id-1").setData(ruleData1).build();
    RateLimitingRuleData ruleData2 =
        RateLimitingRuleData.newBuilder()
            .setName("rule2")
            .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
            .setEnabled(false)
            .setRuleStatus(
                RuleStatus.newBuilder()
                    .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER)
                    .setHidden(true)
                    .setGenerateInternalEvents(false)
                    .build())
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
    RateLimitingRule rule2 = RateLimitingRule.newBuilder().setId("id-2").setData(ruleData2).build();

    UpdateRateLimitingRuleRequest request =
        UpdateRateLimitingRuleRequest.newBuilder().setRuleId("id-2").setData(ruleData1).build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesValidator.validateOrThrow(requestContext, request, List.of(rule1, rule2)));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        status.getDescription(),
        "Update request does not allow to update rule creation source for rule with id: id-2");
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
                                                    .addAllRegions(List.of("IN", "US"))))
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
                                                    .addAllRegions(List.of("IN", "US"))))
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
                                                buildRegionCondition(List.of("IN", "US")))))
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
                                                buildRegionCondition(List.of("IN", "US")))))
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
        StatusRuntimeException.class,
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
                                                    .setValueCondition(
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
                                                buildRegionCondition(List.of("IN", "US")))))
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
                                                    .addUserIds("userId")
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
                                                .addAllRegions(List.of("IN", "US"))))
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
                                                .addAllRegions(List.of("IN", "US"))))
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
