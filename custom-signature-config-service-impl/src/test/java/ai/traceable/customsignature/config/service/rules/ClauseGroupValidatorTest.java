package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import ai.traceable.customsignature.config.service.v1.CityRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.CustomSecRule;
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
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RequestScannerTypeExpression;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.customsignature.config.service.v1.StateRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ClauseGroupValidatorTest {

  private static final String VALID_SEC_RULE =
      "SecRule REQUEST_URI \"@rx test\" \"id:1001,phase:1,deny,msg:'test'\"";

  private ClauseGroupValidator clauseGroupValidator;
  private MockedStatic<ModsecRuleEngineUtils> mockedModsecRuleEngineUtils;

  @BeforeEach
  public void setUp() {
    clauseGroupValidator = new ClauseGroupValidator();
  }

  @AfterEach
  void tearDown() {
    if (mockedModsecRuleEngineUtils != null) {
      mockedModsecRuleEngineUtils.close();
      mockedModsecRuleEngineUtils = null;
    }
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
  void testRegionClauseWithRegionsField() {
    Clause invalidEmpty =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder().addRegions(RegionIdentifier.getDefaultInstance()))
            .build();
    Status status = clauseGroupValidator.validateClause(invalidEmpty, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause invalidEmptyIso =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode(""))))
            .build();
    status = clauseGroupValidator.validateClause(invalidEmptyIso, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validCountry =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US"))))
            .build();
    status = clauseGroupValidator.validateClause(validCountry, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());

    Clause validState =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("California"))))
            .build();
    status = clauseGroupValidator.validateClause(validState, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());

    Clause validCity =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setName("Mumbai"))))
            .build();
    status = clauseGroupValidator.validateClause(validCity, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());

    Clause invalidEmptyStateName =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName(""))))
            .build();
    status = clauseGroupValidator.validateClause(invalidEmptyStateName, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validStateRegex =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setNameRegex("Cal.*"))))
            .build();
    status = clauseGroupValidator.validateClause(validStateRegex, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());

    Clause invalidEmptyCityName =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setName(""))))
            .build();
    status = clauseGroupValidator.validateClause(invalidEmptyCityName, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validCityRegex =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setNameRegex("Mum.*"))))
            .build();
    status = clauseGroupValidator.validateClause(validCityRegex, EventType.EVENT_TYPE_ALLOW);
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

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
            MatchOperator.MATCH_OPERATOR_GREATER_THAN,
            "10.0");
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_HEADERS_COUNT,
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            "match-val-1");
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_COOKIES_COUNT,
            MatchOperator.MATCH_OPERATOR_GREATER_THAN,
            "match-val-2");
    status =
        clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
            MatchOperator.MATCH_OPERATOR_EQUALS,
            "match-val-3");
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_BODY_SIZE, MatchOperator.MATCH_OPERATOR_LESS_THAN, "match-val-4");
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
            MatchOperator.MATCH_OPERATOR_EQUALS,
            Value.newBuilder().setStringValue("str-val").build());
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_HEADERS_COUNT,
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            Value.newBuilder().setStringValue("20").build());
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_COOKIES_COUNT,
            MatchOperator.MATCH_OPERATOR_GREATER_THAN,
            Value.newBuilder().setStringValue("10.4").build());
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_BODY_SIZE,
            MatchOperator.MATCH_OPERATOR_LESS_THAN,
            Value.newBuilder().setNumberValue(5).build());
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_BODY_SIZE,
            MatchOperator.MATCH_OPERATOR_LESS_THAN,
            Value.newBuilder().setNumberValue(6.7).build());
    status = clauseGroupValidator.validateClause(clause, EventType.EVENT_TYPE_TESTING_DETECTION);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
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

  @Test
  void testCustomSecRuleValidation() {
    // missing SecRule keyword
    Clause missingKeyword =
        getCustomSecRuleClause("REQUEST_URI \"@rx test\" \"id:1001,phase:1,deny,msg:'test'\"", "");
    Status status =
        clauseGroupValidator.validateClause(
            missingKeyword, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // missing id action
    Clause missingId =
        getCustomSecRuleClause("SecRule REQUEST_URI \"@rx test\" \"phase:1,deny,msg:'test'\"", "");
    status =
        clauseGroupValidator.validateClause(missingId, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // sanitised_sec_rule must be empty in create/update requests
    Clause nonEmptySanitised =
        getCustomSecRuleClause(VALID_SEC_RULE, "SecRule ARGS \"@rx x\" \"id:1002,phase:1,pass\"");
    status =
        clauseGroupValidator.validateClause(
            nonEmptySanitised, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // modsec validation failure is propagated
    mockedModsecRuleEngineUtils = mockStatic(ModsecRuleEngineUtils.class);
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.modsecValidate(anyString()))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Validation failed for Modsec Rule"));
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
        .thenReturn(Status.OK);

    Clause modsecFailClause = getCustomSecRuleClause(VALID_SEC_RULE, "");
    status =
        clauseGroupValidator.validateClause(
            modsecFailClause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // coraza validation failure is propagated
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.modsecValidate(anyString()))
        .thenReturn(Status.OK);
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
        .thenReturn(
            Status.INVALID_ARGUMENT.withDescription("Exception while validating Rule with Coraza"));

    Clause corazaFailClause = getCustomSecRuleClause(VALID_SEC_RULE, "");
    status =
        clauseGroupValidator.validateClause(
            corazaFailClause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // both engines pass -> OK
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.modsecValidate(anyString()))
        .thenReturn(Status.OK);
    mockedModsecRuleEngineUtils
        .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
        .thenReturn(Status.OK);

    Clause validClause = getCustomSecRuleClause(VALID_SEC_RULE, "");
    status =
        clauseGroupValidator.validateClause(
            validClause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  private Clause getCustomSecRuleClause(String inputSecRule, String sanitisedSecRule) {
    return Clause.newBuilder()
        .setCustomSecRule(
            CustomSecRule.newBuilder()
                .setInputSecRule(inputSecRule)
                .setSanitisedSecRule(sanitisedSecRule)
                .build())
        .build();
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

  private Clause getMatchExpressionClause(
      MatchKey matchKey, MatchOperator matchOperator, Value value) {
    return Clause.newBuilder()
        .setMatchExpression(
            MatchExpression.newBuilder()
                .setMatchKey(matchKey)
                .setMatchOperator(matchOperator)
                .setValue(value))
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

  @Test
  void testSecRuleIdUniqueness_NoSecRules() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                            .setValue(Value.newBuilder().setStringValue("/api/test"))))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSecRuleIdUniqueness_SingleSecRule() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSecRuleIdUniqueness_MultipleSecRulesWithDifferentIds() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.sh$\" \"id:9501,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule ARGS \"@contains malicious\" \"id:9502,phase:2,block\"")))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSecRuleIdUniqueness_DuplicateIdsAtSameLevel() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.sh$\" \"id:9500,phase:2,deny\"")))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Duplicate sec_rule ID '9500' found within the rule. Each sec_rule must have a unique ID within the same custom signature rule.",
        status.getDescription());
  }

  @Test
  void testSecRuleIdUniqueness_DuplicateIdsAcrossNestingLevels() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                                            .setMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                            .setValue(
                                                Value.newBuilder().setStringValue("^/upload"))))
                            .addClauses(
                                Clause.newBuilder()
                                    .setCustomSecRule(
                                        CustomSecRule.newBuilder()
                                            .setInputSecRule(
                                                "SecRule FILES \"@rx \\\\.sh$\" \"id:9500,phase:2,deny\"")))))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Duplicate sec_rule ID '9500' found within the rule. Each sec_rule must have a unique ID within the same custom signature rule.",
        status.getDescription());
  }

  @Test
  void testSecRuleIdUniqueness_NestedClauseGroupsWithDifferentIds() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)
                            .addClauses(
                                Clause.newBuilder()
                                    .setCustomSecRule(
                                        CustomSecRule.newBuilder()
                                            .setInputSecRule(
                                                "SecRule FILES \"@rx \\\\.sh$\" \"id:9501,phase:2,deny\"")))
                            .addClauses(
                                Clause.newBuilder()
                                    .setClauseGroup(
                                        ClauseGroup.newBuilder()
                                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setCustomSecRule(
                                                        CustomSecRule.newBuilder()
                                                            .setInputSecRule(
                                                                "SecRule ARGS \"@contains test\" \"id:9502,phase:2,block\"")))))))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSecRuleIdUniqueness_ThreeSecRulesWithOneDuplicate() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.exe$\" \"id:9500,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule FILES \"@rx \\\\.sh$\" \"id:9501,phase:2,deny\"")))
            .addClauses(
                Clause.newBuilder()
                    .setCustomSecRule(
                        CustomSecRule.newBuilder()
                            .setInputSecRule(
                                "SecRule ARGS \"@contains test\" \"id:9500,phase:2,block\"")))
            .build();

    Status status =
        clauseGroupValidator.validateClauseGroup(clauseGroup, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Duplicate sec_rule ID '9500' found within the rule. Each sec_rule must have a unique ID within the same custom signature rule.",
        status.getDescription());
  }
}
