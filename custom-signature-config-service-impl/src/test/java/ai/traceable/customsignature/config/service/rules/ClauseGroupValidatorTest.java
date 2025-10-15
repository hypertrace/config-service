package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocity;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocityExpression;
import ai.traceable.customsignature.config.service.v1.IpAsnExpression;
import ai.traceable.customsignature.config.service.v1.IpConnectionType;
import ai.traceable.customsignature.config.service.v1.IpConnectionTypeExpression;
import ai.traceable.customsignature.config.service.v1.IpOrganisationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationSeverity;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RequestScannerTypeExpression;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClauseGroupValidatorTest {

  private ClauseGroupValidator clauseGroupValidator;

  @BeforeEach
  public void setUp() {
    clauseGroupValidator = new ClauseGroupValidator();
  }

  @Test
  void testValidIpOrganisationClause() {
    Clause validClause = getIpOrganisationClause(true, List.of("^reg"));
    assertDoesNotThrow(
        () -> clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpOrganisationClause(false, List.of("]["));
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testValidIpAsnClause() {
    Clause validClause = getIpAsnClause(true, List.of("^reg"));
    assertDoesNotThrow(
        () -> clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpAsnClause(false, List.of("]["));
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testValidIpAbuseVelocityClause() {
    Clause validClause = getIpAbuseVelocityClause(IpAbuseVelocity.IP_ABUSE_VELOCITY_LOW);
    assertDoesNotThrow(
        () -> clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpAbuseVelocityClause(IpAbuseVelocity.IP_ABUSE_VELOCITY_UNSPECIFIED);
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testIpScannerTypeClause() {
    Clause validClause = getRequestScannerTypeClause(List.of("Scanner1", "Scanner2"));
    assertDoesNotThrow(
        () -> clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause1 = getRequestScannerTypeClause(Collections.emptyList());
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 = getRequestScannerTypeClause(List.of("", "Scanner1"));
    Status status = clauseGroupValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testRegionClause() {
    Clause invalidClause =
        Clause.newBuilder().setRegionExpression(RegionExpression.newBuilder()).build();
    Status status = clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    invalidClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(RegionExpression.Region.getDefaultInstance()))
            .build();
    status = clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    invalidClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(
                        RegionExpression.Region.newBuilder().setCountryIsoCode("")))
            .build();
    status = clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(
                        RegionExpression.Region.newBuilder().setCountryIsoCode("ssfsd")))
            .build();
    status = clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testIpTypeClause() {
    Clause invalidClause1 =
        Clause.newBuilder().setIpTypeExpression(IpTypeExpression.newBuilder()).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 =
        Clause.newBuilder()
            .setIpTypeExpression(
                IpTypeExpression.newBuilder().addIpTypes(IpType.IP_TYPE_UNSPECIFIED))
            .build();
    Status status = clauseGroupValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setIpTypeExpression(IpTypeExpression.newBuilder().addIpTypes(IpType.IP_TYPE_BOT))
            .build();
    status = clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testIpReputationClause() {
    Clause invalidClause =
        Clause.newBuilder().setIpReputationExpression(IpReputationExpression.newBuilder()).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));

    Clause validClause =
        Clause.newBuilder()
            .setIpReputationExpression(
                IpReputationExpression.newBuilder()
                    .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
            .build();
    assertDoesNotThrow(
        () -> clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));
  }

  @Test
  void testIpConnectionTypeClause() {
    Clause invalidClause1 =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(IpConnectionTypeExpression.newBuilder())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(
                IpConnectionTypeExpression.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED))
            .build();
    Status status = clauseGroupValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(
                IpConnectionTypeExpression.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_CORPORATE))
            .build();
    status = clauseGroupValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testScopeClause() {
    Clause invalidClause1 =
        Clause.newBuilder().setScopeExpression(ScopeExpression.newBuilder().build()).build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        clauseGroupValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW).getCode());

    Clause invalidClause2 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setEntityScope(
                        ScopeExpression.EntityScope.newBuilder()
                            .setEntityType(ScopeExpression.EntityType.ENTITY_TYPE_API)
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause3 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setEntityScope(
                        ScopeExpression.EntityScope.newBuilder().addEntityIds("entity-id").build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause3, EventType.EVENT_TYPE_ALLOW));

    Clause validEntityScope =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setEntityScope(
                        ScopeExpression.EntityScope.newBuilder()
                            .setEntityType(ScopeExpression.EntityType.ENTITY_TYPE_API)
                            .addEntityIds("entity-id")
                            .build())
                    .build())
            .build();
    assertEquals(
        Status.OK.getCode(),
        clauseGroupValidator
            .validateClause(validEntityScope, EventType.EVENT_TYPE_ALLOW)
            .getCode());

    Clause invalidClause4 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setLabelScope(
                        ScopeExpression.LabelScope.newBuilder().addLabelIds("label-id").build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause4, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause5 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setLabelScope(
                        ScopeExpression.LabelScope.newBuilder()
                            .setLabelType(ScopeExpression.LabelType.LABEL_TYPE_API)
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause5, EventType.EVENT_TYPE_ALLOW));

    Clause validLabelScope =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setLabelScope(
                        ScopeExpression.LabelScope.newBuilder()
                            .setLabelType(ScopeExpression.LabelType.LABEL_TYPE_API)
                            .addLabelIds("label-id")
                            .build())
                    .build())
            .build();
    assertEquals(
        Status.OK.getCode(),
        clauseGroupValidator.validateClause(validLabelScope, EventType.EVENT_TYPE_ALLOW).getCode());

    Clause invalidClause6 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setUrlScope(ScopeExpression.UrlScope.newBuilder().build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseGroupValidator.validateClause(invalidClause6, EventType.EVENT_TYPE_ALLOW));

    Clause validUrlScope =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setUrlScope(
                        ScopeExpression.UrlScope.newBuilder().addUrlRegexes("url-regex-1").build())
                    .build())
            .build();
    assertEquals(
        Status.OK.getCode(),
        clauseGroupValidator.validateClause(validUrlScope, EventType.EVENT_TYPE_ALLOW).getCode());
  }

  @Test
  void testMatchExpressionClause() {
    Clause clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_UNSPECIFIED, MatchOperator.MATCH_OPERATOR_UNSPECIFIED, "");
    Status status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_COOKIE_VALUE,
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            "cookie-value");
    status =
        clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_HEADER_VALUE,
            MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
            "header-value");
    status =
        clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT, MatchOperator.MATCH_OPERATOR_GREATER_THAN, "1");
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testLhsRhsKeysExpressionClause() {
    // invalid first level MatchOperator (using the now deprecated flow)
    Clause lhsRhsKeysExpressionClause1 =
        getLhsRhsKeysExpressionClause(
            MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
            MatchKey.MATCH_KEY_HEADER_NAME,
            MatchOperator.MATCH_OPERATOR_CONTAINS,
            "lhs-match-value-1",
            MatchKey.MATCH_KEY_COOKIE_NAME,
            MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
            "rhs-match-value-1");
    Status status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause1, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // invalid second level MatchOperator (using the now deprecated flow)
    Clause lhsRhsKeysExpressionClause2 =
        getLhsRhsKeysExpressionClause(
            MatchOperator.MATCH_OPERATOR_EQUALS,
            MatchKey.MATCH_KEY_HEADER_NAME,
            MatchOperator.MATCH_OPERATOR_GREATER_THAN,
            "100",
            MatchKey.MATCH_KEY_COOKIE_NAME,
            MatchOperator.MATCH_OPERATOR_LESS_THAN,
            "10");
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause2, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // same LhsKeyExpression and RhsKeyExpression (using the now deprecated flow)
    Clause lhsRhsKeysExpressionClause3 =
        getLhsRhsKeysExpressionClause(
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            MatchKey.MATCH_KEY_COOKIE_NAME,
            MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
            "lhs-rhs-match-value",
            MatchKey.MATCH_KEY_COOKIE_NAME,
            MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
            "lhs-rhs-match-value");
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause3, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // key-null MatchKeys (using the now deprecated flow)
    Clause lhsRhsKeysExpressionClause4 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setLhsKeyExpression(
                        MatchExpression.newBuilder().setMatchKey(MatchKey.MATCH_KEY_URL))
                    .setRhsKeyExpression(
                        MatchExpression.newBuilder().setMatchKey(MatchKey.MATCH_KEY_HTTP_METHOD))
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_NOT_EQUAL))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause4, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    // LhsKeyExpression is present, but RhsKeyExpression isn't (using the now deprecated flow)
    Clause lhsRhsKeysExpression5 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setLhsKeyExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_NAME)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setValue(Value.newBuilder().setStringValue("str-value")))
                    .clearRhsKeyExpression())
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpression5, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // LhsExpression is present, but RhsExpression isn't (using the newly added oneof fields)
    Clause lhsRhsKeysExpression6 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setKeyLhsExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_COOKIE_NAME)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setValue(Value.newBuilder().setStringValue("str-value-2")))
                    .clearAttributeRhsExpression()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpression6, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // RhsExpression is present, but LhsExpression isn't (using the newly added oneof fields)
    Clause lhsRhsKeysExpression7 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .clearKeyLhsExpression()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setAttributeRhsExpression(
                        getStringCondition(MatchOperator.MATCH_OPERATOR_CONTAINS, "str-value-3")))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpression7, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // same AttributeLhsExpression and AttributeRhsExpression (using the newly added oneof fields)
    Clause lhsRhsKeysExpressionClause8 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setAttributeLhsExpression(
                        getStringCondition(MatchOperator.MATCH_OPERATOR_CONTAINS, "str-value-1"))
                    .setAttributeRhsExpression(
                        getStringCondition(MatchOperator.MATCH_OPERATOR_CONTAINS, "str-value-1")))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause8, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // valid case 1 of different categories of match keys of the now deprecated flow
    Clause lhsRhsKeysExpressionClause9 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setLhsKeyExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_COOKIE_NAME)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                            .setValue(Value.newBuilder().setStringValue("cookie-name-regex")))
                    .setRhsKeyExpression(
                        MatchExpression.newBuilder().setMatchKey(MatchKey.MATCH_KEY_HOST))
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause9, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    // valid case 2 of different categories of match keys of the newly added flow
    Clause lhsRhsKeysExpressionClause10 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setKeyLhsExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_STATUS_CODE)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE))
                    .setKeyRhsExpression(
                        MatchExpression.newBuilder()
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_NAME)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                            .setValue(Value.newBuilder().setStringValue("header-name-str"))))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause10, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    // valid case 3 (using the now deprecated flow)
    Clause lhsRhsKeysExpressionClause11 =
        getLhsRhsKeysExpressionClause(
            MatchOperator.MATCH_OPERATOR_EQUALS,
            MatchKey.MATCH_KEY_HEADER_NAME,
            MatchOperator.MATCH_OPERATOR_CONTAINS,
            "lhs-match-value-3",
            MatchKey.MATCH_KEY_COOKIE_NAME,
            MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
            "rhs-match-value-3");
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause11, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    // valid case 4 (using the newly added oneof fields)
    Clause lhsRhsKeysExpressionClause12 =
        Clause.newBuilder()
            .setLhsRhsKeysExpression(
                LhsRhsKeysExpression.newBuilder()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                    .setAttributeLhsExpression(
                        getStringCondition(MatchOperator.MATCH_OPERATOR_NOT_CONTAIN, "str-value-3"))
                    .setKeyRhsExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_COOKIE_NAME)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setValue(Value.newBuilder().setStringValue("str-value-4"))))
            .build();
    status =
        clauseGroupValidator.validateClause(
            lhsRhsKeysExpressionClause12, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  private Clause getLhsRhsKeysExpressionClause(
      MatchOperator lhsRhsKeysMatchOperator,
      MatchKey lhsMatchKey,
      MatchOperator lhsMatchOperator,
      String lhsMatchValue,
      MatchKey rhsMatchKey,
      MatchOperator rhsMatchOperator,
      String rhsMatchValue) {
    return Clause.newBuilder()
        .setLhsRhsKeysExpression(
            LhsRhsKeysExpression.newBuilder()
                .setLhsKeyExpression(
                    MatchExpression.newBuilder()
                        .setMatchKey(lhsMatchKey)
                        .setMatchOperator(lhsMatchOperator)
                        .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                        .setValue(Value.newBuilder().setStringValue(lhsMatchValue)))
                .setRhsKeyExpression(
                    MatchExpression.newBuilder()
                        .setMatchKey(rhsMatchKey)
                        .setMatchOperator(rhsMatchOperator)
                        .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                        .setValue(Value.newBuilder().setStringValue(rhsMatchValue)))
                .setMatchOperator(lhsRhsKeysMatchOperator))
        .build();
  }

  private Clause getMatchExpressionClause(
      MatchKey matchKey, MatchOperator matchOperator, String matchValue) {
    return Clause.newBuilder()
        .setMatchExpression(
            MatchExpression.newBuilder()
                .setMatchKey(matchKey)
                .setMatchOperator(matchOperator)
                .setMatchValue(matchValue))
        .build();
  }

  private StringCondition getStringCondition(MatchOperator matchOperator, String value) {
    return StringCondition.newBuilder().setOperator(matchOperator).setValue(value).build();
  }

  private Clause getRequestScannerTypeClause(List<String> scannerTypes) {
    return Clause.newBuilder()
        .setRequestScannerTypeExpression(
            RequestScannerTypeExpression.newBuilder().addAllScannerTypes(scannerTypes).build())
        .build();
  }

  private Clause getIpAbuseVelocityClause(IpAbuseVelocity ipAbuseVelocity) {
    return Clause.newBuilder()
        .setIpAbuseVelocityExpression(
            IpAbuseVelocityExpression.newBuilder().setMinIpAbuseVelocity(ipAbuseVelocity).build())
        .build();
  }

  private Clause getIpOrganisationClause(boolean exclude, List<String> regexes) {
    return Clause.newBuilder()
        .setIpOrganisationExpression(
            IpOrganisationExpression.newBuilder()
                .setExclude(exclude)
                .addAllIpOrganisationRegexes(regexes)
                .build())
        .build();
  }

  private Clause getIpAsnClause(boolean exclude, List<String> regexes) {
    return Clause.newBuilder()
        .setIpAsnExpression(
            IpAsnExpression.newBuilder().setExclude(exclude).addAllIpAsnRegexes(regexes).build())
        .build();
  }
}
