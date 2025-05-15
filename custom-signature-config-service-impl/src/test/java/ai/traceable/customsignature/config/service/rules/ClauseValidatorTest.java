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
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RequestScannerTypeExpression;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClauseValidatorTest {

  private ClauseValidator clauseValidator;

  @BeforeEach
  public void setUp() {
    clauseValidator = new ClauseValidator();
  }

  @Test
  void testValidIpOrganisationClause() {
    Clause validClause = getIpOrganisationClause(true, List.of("^reg"));
    assertDoesNotThrow(
        () -> clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpOrganisationClause(false, List.of("]["));
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testValidIpAsnClause() {
    Clause validClause = getIpAsnClause(true, List.of("^reg"));
    assertDoesNotThrow(
        () -> clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpAsnClause(false, List.of("]["));
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testValidIpAbuseVelocityClause() {
    Clause validClause = getIpAbuseVelocityClause(IpAbuseVelocity.IP_ABUSE_VELOCITY_LOW);
    assertDoesNotThrow(
        () -> clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause = getIpAbuseVelocityClause(IpAbuseVelocity.IP_ABUSE_VELOCITY_UNSPECIFIED);
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testIpScannerTypeClause() {
    Clause validClause = getRequestScannerTypeClause(List.of("Scanner1", "Scanner2"));
    assertDoesNotThrow(
        () -> clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause1 = getRequestScannerTypeClause(Collections.emptyList());
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 = getRequestScannerTypeClause(List.of("", "Scanner1"));
    Status status = clauseValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testRegionClause() {
    Clause invalidClause =
        Clause.newBuilder().setRegionExpression(RegionExpression.newBuilder()).build();
    Status status = clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    invalidClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(RegionExpression.Region.getDefaultInstance()))
            .build();
    status = clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    invalidClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(
                        RegionExpression.Region.newBuilder().setCountryIsoCode("")))
            .build();
    status = clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(
                        RegionExpression.Region.newBuilder().setCountryIsoCode("ssfsd")))
            .build();
    status = clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testIpTypeClause() {
    Clause invalidClause1 =
        Clause.newBuilder().setIpTypeExpression(IpTypeExpression.newBuilder()).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 =
        Clause.newBuilder()
            .setIpTypeExpression(
                IpTypeExpression.newBuilder().addIpTypes(IpType.IP_TYPE_UNSPECIFIED))
            .build();
    Status status = clauseValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setIpTypeExpression(IpTypeExpression.newBuilder().addIpTypes(IpType.IP_TYPE_BOT))
            .build();
    status = clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testIpReputationClause() {
    Clause invalidClause =
        Clause.newBuilder().setIpReputationExpression(IpReputationExpression.newBuilder()).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseValidator.validateClause(invalidClause, EventType.EVENT_TYPE_ALLOW));

    Clause validClause =
        Clause.newBuilder()
            .setIpReputationExpression(
                IpReputationExpression.newBuilder()
                    .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
            .build();
    assertDoesNotThrow(
        () -> clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW));
  }

  @Test
  void testIpConnectionTypeClause() {
    Clause invalidClause1 =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(IpConnectionTypeExpression.newBuilder())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW));

    Clause invalidClause2 =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(
                IpConnectionTypeExpression.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED))
            .build();
    Status status = clauseValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    Clause validClause =
        Clause.newBuilder()
            .setIpConnectionTypeExpression(
                IpConnectionTypeExpression.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_CORPORATE))
            .build();
    status = clauseValidator.validateClause(validClause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testScopeClause() {
    Clause invalidClause1 =
        Clause.newBuilder().setScopeExpression(ScopeExpression.newBuilder().build()).build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        clauseValidator.validateClause(invalidClause1, EventType.EVENT_TYPE_ALLOW).getCode());

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
        () -> clauseValidator.validateClause(invalidClause2, EventType.EVENT_TYPE_ALLOW));

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
        () -> clauseValidator.validateClause(invalidClause3, EventType.EVENT_TYPE_ALLOW));

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
        clauseValidator.validateClause(validEntityScope, EventType.EVENT_TYPE_ALLOW).getCode());

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
        () -> clauseValidator.validateClause(invalidClause4, EventType.EVENT_TYPE_ALLOW));

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
        () -> clauseValidator.validateClause(invalidClause5, EventType.EVENT_TYPE_ALLOW));

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
        clauseValidator.validateClause(validLabelScope, EventType.EVENT_TYPE_ALLOW).getCode());

    Clause invalidClause6 =
        Clause.newBuilder()
            .setScopeExpression(
                ScopeExpression.newBuilder()
                    .setUrlScope(ScopeExpression.UrlScope.newBuilder().build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> clauseValidator.validateClause(invalidClause6, EventType.EVENT_TYPE_ALLOW));

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
        clauseValidator.validateClause(validUrlScope, EventType.EVENT_TYPE_ALLOW).getCode());
  }

  @Test
  void testMatchExpressionClause() {
    Clause clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_UNSPECIFIED, MatchOperator.MATCH_OPERATOR_UNSPECIFIED, "");
    Status status = clauseValidator.validateClause(clause, EventType.EVENT_TYPE_ALLOW);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_COOKIE_VALUE,
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            "cookie-value");
    status = clauseValidator.validateClause(clause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_HEADER_VALUE,
            MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
            "header-value");
    status = clauseValidator.validateClause(clause, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    clause =
        getMatchExpressionClause(
            MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT, MatchOperator.MATCH_OPERATOR_GREATER_THAN, "1");
    status = clauseValidator.validateClause(clause, EventType.EVENT_TYPE_NORMAL_DETECTION);
    assertEquals(Status.OK.getCode(), status.getCode());
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
