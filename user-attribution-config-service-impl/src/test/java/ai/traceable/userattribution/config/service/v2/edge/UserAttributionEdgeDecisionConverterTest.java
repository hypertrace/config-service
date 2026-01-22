package ai.traceable.userattribution.config.service.v2.edge;

import static ai.traceable.userattribution.config.service.v2.KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class UserAttributionEdgeDecisionConverterTest {

  @Test
  void convert_ordersRulesByRankAscending_andAppendsIpFallbackLast() {
    UserAttributionEdgeDecisionConverter converter = new UserAttributionEdgeDecisionConverter();
    UserAttributionRule rank2Rule = buildHeaderUserIdRule("rule-2", 2, "x-rank-2");
    UserAttributionRule rank1Rule = buildHeaderUserIdRule("rule-1", 1, "x-rank-1");

    EdgeDecisionEngineConfig config = converter.convert(List.of(rank2Rule, rank1Rule));

    VariableDerivationMapping userIdVar =
        config.getCommonVariablesList().stream()
            .filter(v -> v.getName().equals("enduser.id"))
            .findFirst()
            .orElse(null);
    assertNotNull(userIdVar);

    List<String> jexlExpressions =
        userIdVar.getRulesList().stream()
            .map(r -> r.getTransformationConfig().getJexlExpression().getJexlExpression())
            .collect(Collectors.toList());

    assertEquals("$s.getRequestHeaders().get('x-rank-1')", jexlExpressions.get(0));
    assertEquals("$s.getRequestHeaders().get('x-rank-2')", jexlExpressions.get(1));
    assertEquals("$s.getIpAddress()", jexlExpressions.get(jexlExpressions.size() - 1));
  }

  @Test
  void convert_skipsCustomProjectionUserIdRule_andStillReturnsIpFallback() {
    UserAttributionEdgeDecisionConverter converter = new UserAttributionEdgeDecisionConverter();
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setCustomProjection(CustomProjection.newBuilder().setCustomJson("{\"a\":1}")))
            .build();
    UserAttributionRule rule =
        UserAttributionRule.newBuilder().setId("custom").setRank(1).setData(data).build();

    EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

    VariableDerivationMapping userIdVar =
        config.getCommonVariablesList().stream()
            .filter(v -> v.getName().equals("enduser.id"))
            .findFirst()
            .orElse(null);
    assertNotNull(userIdVar);

    List<String> jexlExpressions =
        userIdVar.getRulesList().stream()
            .map(r -> r.getTransformationConfig().getJexlExpression().getJexlExpression())
            .collect(Collectors.toList());

    assertEquals(1, jexlExpressions.size());
    assertEquals("$s.getIpAddress()", jexlExpressions.get(0));
  }

  @Test
  void convert_emitsRoleScopeAndAuthTypesAsList_andSplitsCommaSeparatedValues() {
    UserAttributionEdgeDecisionConverter converter = new UserAttributionEdgeDecisionConverter();

    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setUserRoleRule(
                UserAttributionTokenRule.newBuilder()
                    .setLiteralValueProjection(
                        ai.traceable.userattribution.config.service.v2.LiteralValueProjection
                            .newBuilder()
                            .setLiteralValue(
                                ai.traceable.userattribution.config.service.v2.LiteralValue
                                    .newBuilder()
                                    .setStringValue("admin, user"))))
            .setUserScopeRule(
                UserAttributionTokenRule.newBuilder()
                    .setLiteralValueProjection(
                        ai.traceable.userattribution.config.service.v2.LiteralValueProjection
                            .newBuilder()
                            .setLiteralValue(
                                ai.traceable.userattribution.config.service.v2.LiteralValue
                                    .newBuilder()
                                    .setStringValue("read,write"))))
            .setAuthTypeRule(
                UserAttributionTokenRule.newBuilder()
                    .setLiteralValueProjection(
                        ai.traceable.userattribution.config.service.v2.LiteralValueProjection
                            .newBuilder()
                            .setLiteralValue(
                                ai.traceable.userattribution.config.service.v2.LiteralValue
                                    .newBuilder()
                                    .setStringValue("jwt"))))
            .build();

    UserAttributionRule rule =
        UserAttributionRule.newBuilder().setId("r1").setRank(1).setData(data).build();

    EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

    VariableDerivationMapping roleVar =
        config.getCommonVariablesList().stream()
            .filter(v -> v.getName().equals("enduser.role"))
            .findFirst()
            .orElse(null);
    VariableDerivationMapping scopeVar =
        config.getCommonVariablesList().stream()
            .filter(v -> v.getName().equals("enduser.scope"))
            .findFirst()
            .orElse(null);
    VariableDerivationMapping authTypesVar =
        config.getCommonVariablesList().stream()
            .filter(v -> v.getName().equals("traceableai.auth.types"))
            .findFirst()
            .orElse(null);

    assertNotNull(roleVar);
    assertNotNull(scopeVar);
    assertNotNull(authTypesVar);

    assertEquals(
        FieldType.FIELD_TYPE_LIST, roleVar.getRules(0).getTransformationConfig().getOutputType());
    assertEquals(
        FieldType.FIELD_TYPE_LIST, scopeVar.getRules(0).getTransformationConfig().getOutputType());
    assertEquals(
        FieldType.FIELD_TYPE_LIST,
        authTypesVar.getRules(0).getTransformationConfig().getOutputType());

    assertEquals(
        "java.util.Arrays.asList('admin, user'.split(\"\\s*,\\s*\"))",
        roleVar.getRules(0).getTransformationConfig().getJexlExpression().getJexlExpression());
    assertEquals(
        "java.util.Arrays.asList('read,write'.split(\"\\s*,\\s*\"))",
        scopeVar.getRules(0).getTransformationConfig().getJexlExpression().getJexlExpression());
    assertEquals(
        "java.util.Arrays.asList('jwt'.split(\"\\s*,\\s*\"))",
        authTypesVar.getRules(0).getTransformationConfig().getJexlExpression().getJexlExpression());
  }

  private static UserAttributionRule buildHeaderUserIdRule(String id, int rank, String headerName) {
    UserAttributionRuleData data =
        UserAttributionRuleData.newBuilder()
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey(headerName)))))
            .build();

    return UserAttributionRule.newBuilder().setId(id).setRank(rank).setData(data).build();
  }
}
