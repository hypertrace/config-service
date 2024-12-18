package ai.traceable.edge.decision.config.service.aggregator.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.GenericMatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CheckAndAddVariableToRuleTest {

  private CheckAndAddVariableToRule checkAndAddVariableToRule;

  @BeforeEach
  void setup() {
    checkAndAddVariableToRule = new CheckAndAddVariableToRule();
  }

  @Test
  void testCheckAndAddVariableToRule_VariableFound() {
    String variableName = "USER";
    EdgeDecisionRule edgeDecisionRule =
        EdgeDecisionRule.newBuilder()
            .setId("rule1")
            .setRuleDefinition(
                EdgeDecisionRuleDefinition.newBuilder()
                    .setSignatureRule(
                        SignatureRule.newBuilder()
                            .setMatchCondition(
                                MatchCondition.newBuilder()
                                    .setGenericMatchCondition(
                                        GenericMatchCondition.newBuilder()
                                            .setJexlExpression(
                                                JexlExpressionConfig.newBuilder()
                                                    .setJexlExpression(
                                                        "$USER + $s.getHeaders().get('authorization')")))))
                    .addRuleVariables(
                        VariableDerivationMapping.newBuilder().setName("existingVariable").build())
                    .build())
            .build();
    EdgeDecisionEngineConfig edgeDecisionEngineConfig =
        EdgeDecisionEngineConfig.newBuilder()
            .setId("config-1")
            .addDecisionRules(edgeDecisionRule)
            .addCommonVariables(VariableDerivationMapping.newBuilder().setName("existingVariable"))
            .build();
    VariableDerivationMapping variableMapping =
        VariableDerivationMapping.newBuilder()
            .setName(variableName)
            .addRules(
                DerivationRule.newBuilder()
                    .setConditionExpression(
                        JexlExpressionConfig.newBuilder()
                            .setJexlExpression("$s.getHeaders().get('authorization')")))
            .build();

    edgeDecisionEngineConfig =
        checkAndAddVariableToRule.checkAndAddVariableToRule(
            edgeDecisionEngineConfig, variableName, variableMapping);

    assertEquals(2, edgeDecisionEngineConfig.getCommonVariablesCount());
    assertEquals(variableMapping, edgeDecisionEngineConfig.getCommonVariablesList().get(1));

    VariableDerivationMapping variableMapping2 =
        VariableDerivationMapping.newBuilder().setName("existingVariable").build();

    edgeDecisionEngineConfig =
        checkAndAddVariableToRule.checkAndAddVariableToRule(
            edgeDecisionEngineConfig, "existingVariable", variableMapping2);
    // Test existing variable has no effect
    assertEquals(2, edgeDecisionEngineConfig.getCommonVariablesCount());
    assertEquals("USER", edgeDecisionEngineConfig.getCommonVariablesList().get(1).getName());

    variableName = "NON_EXISTENT_VARIABLE";
    variableMapping = VariableDerivationMapping.newBuilder().setName(variableName).build();

    edgeDecisionEngineConfig =
        checkAndAddVariableToRule.checkAndAddVariableToRule(
            edgeDecisionEngineConfig, variableName, variableMapping);

    assertEquals(2, edgeDecisionEngineConfig.getCommonVariablesCount());
    assertEquals(
        "existingVariable", edgeDecisionEngineConfig.getCommonVariablesList().get(0).getName());
    assertEquals("USER", edgeDecisionEngineConfig.getCommonVariablesList().get(1).getName());
  }
}
