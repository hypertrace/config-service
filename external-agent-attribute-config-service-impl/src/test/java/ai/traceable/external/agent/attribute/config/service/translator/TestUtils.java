package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.FirstMatchingProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRules;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

class TestUtils {

  private static final Parser PARSER = JsonFormat.parser();

  static List<AttributeRule> getExpectedAttributeRules(String filename) throws IOException {
    FirstMatchingProjector.Builder projectorBuilder = FirstMatchingProjector.newBuilder();
    PARSER.merge(
        new BufferedReader(
            new InputStreamReader(TestUtils.class.getClassLoader().getResourceAsStream(filename))),
        projectorBuilder);
    return projectorBuilder.build().getAttributeRulesList();
  }

  static AttributeRule getExpectedAttributeRule(String filename) throws IOException {
    AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
    PARSER.merge(
        new BufferedReader(
            new InputStreamReader(TestUtils.class.getClassLoader().getResourceAsStream(filename))),
        attributeRuleBuilder);
    return attributeRuleBuilder.build();
  }

  static AgentAttributeRules getExpectedAgentAttributeRules(String filename) throws IOException {
    AgentAttributeRules.Builder agentAttributeRulesBuilder = AgentAttributeRules.newBuilder();
    PARSER.merge(
        new BufferedReader(
            new InputStreamReader(TestUtils.class.getClassLoader().getResourceAsStream(filename))),
        agentAttributeRulesBuilder);
    return agentAttributeRulesBuilder.build();
  }

  static UserAttributionRule getUserAttributionRule(String filename) throws IOException {
    UserAttributionRule.Builder ruleBuilder = UserAttributionRule.newBuilder();
    PARSER.merge(
        new BufferedReader(
            new InputStreamReader(TestUtils.class.getClassLoader().getResourceAsStream(filename))),
        ruleBuilder);
    return ruleBuilder.build();
  }
}
