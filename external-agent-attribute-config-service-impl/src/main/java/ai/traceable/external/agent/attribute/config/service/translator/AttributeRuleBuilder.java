package ai.traceable.external.agent.attribute.config.service.translator;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_TYPES_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.AUTH_TYPES_RULE_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.END_USER_ID_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.END_USER_ID_RULE_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.END_USER_ROLE_ATTRIBUTE_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.END_USER_ROLE_RULE_ATTRIBUTE_KEY;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Action.AttributeAddition;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Action.AttributeArrayAppend;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.AttributeProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.Base64Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.CookieProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.FirstMatchingProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.JsonProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.JwtProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.NoOpProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.ParsedObjectKeyRule;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.RegexCaptureGroupProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule.Projector.StaticValueProjector;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AttributeRuleBuilder {

  AttributeRule buildActionAttributeRuleForUserId(String ruleId) {
    return AttributeRule.newBuilder()
        .addInitialActions(buildAttributeAdditionAction(END_USER_ID_ATTRIBUTE_KEY))
        .addInitialActions(buildAttributeAdditionAction(END_USER_ID_RULE_ATTRIBUTE_KEY, ruleId))
        .build();
  }

  AttributeRule buildActionAttributeRuleForUserRole(String ruleId) {
    return AttributeRule.newBuilder()
        .addInitialActions(buildAttributeAdditionAction(END_USER_ROLE_ATTRIBUTE_KEY))
        .addInitialActions(buildAttributeAdditionAction(END_USER_ROLE_RULE_ATTRIBUTE_KEY, ruleId))
        .build();
  }

  AttributeRule buildActionAttributeRuleForAuthType(String authType, String ruleId) {
    return AttributeRule.newBuilder()
        .addAllInitialActions(buildActionsForAuthType(authType, ruleId))
        .build();
  }

  List<Action> buildActionsForAuthType(String authType, String ruleId) {
    return List.of(
        buildAttributeAppendAction(AUTH_TYPES_ATTRIBUTE_KEY, authType),
        buildAttributeAppendAction(AUTH_TYPES_RULE_ATTRIBUTE_KEY, ruleId));
  }

  private Action buildAttributeAdditionAction(String key) {
    return Action.newBuilder()
        .setAttributeAddition(
            AttributeAddition.newBuilder()
                .setAttributeKey(key)
                .setValueProjectionRule(
                    AttributeRule.newBuilder()
                        .setProjector(
                            Projector.newBuilder()
                                .setNoOpProjector(NoOpProjector.getDefaultInstance()))))
        .build();
  }

  private Action buildAttributeAdditionAction(String key, String value) {
    return Action.newBuilder()
        .setAttributeAddition(
            AttributeAddition.newBuilder()
                .setAttributeKey(key)
                .setValueProjectionRule(
                    AttributeRule.newBuilder()
                        .setProjector(
                            Projector.newBuilder()
                                .setValueProjector(
                                    StaticValueProjector.newBuilder().setValue(value)))))
        .build();
  }

  private Action buildAttributeAppendAction(String key, String value) {
    return Action.newBuilder()
        .setAttributeArrayAppend(
            AttributeArrayAppend.newBuilder()
                .setAttributeKey(key)
                .setValueProjectionRule(
                    AttributeRule.newBuilder()
                        .setProjector(
                            Projector.newBuilder()
                                .setValueProjector(
                                    StaticValueProjector.newBuilder().setValue(value)))))
        .build();
  }

  AttributeRule buildRuleForAttribute(String attributeKey, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setAttributeProjector(
                    AttributeProjector.newBuilder()
                        .setAttributeKey(attributeKey)
                        .setAttributeRule(childRule)))
        .build();
  }

  AttributeRule buildRuleForRegexCaptureGroup(String regexCaptureGroup, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setRegexCaptureGroupProjector(
                    RegexCaptureGroupProjector.newBuilder()
                        .setRegexCaptureGroup(regexCaptureGroup)
                        .setAttributeRule(childRule)))
        .build();
  }

  AttributeRule buildRuleForBase64(AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setBase64Projector(Base64Projector.newBuilder().setAttributeRule(childRule)))
        .build();
  }

  AttributeRule buildRuleForJsonPath(String jsonPath, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setJsonProjector(
                    JsonProjector.newBuilder()
                        .addJsonPathRules(
                            ParsedObjectKeyRule.newBuilder()
                                .setKey(jsonPath)
                                .setAttributeRule(childRule))))
        .build();
  }

  AttributeRule buildRuleForJwtClaim(String claim, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setJwtProjector(
                    JwtProjector.newBuilder()
                        .addClaimRules(
                            ParsedObjectKeyRule.newBuilder()
                                .setKey(claim)
                                .setAttributeRule(childRule))))
        .build();
  }

  AttributeRule buildRuleForParsingJwt(List<Action> beforeChildrenActions) {
    return AttributeRule.newBuilder()
        .addAllBeforeChildrenActions(beforeChildrenActions)
        .setProjector(Projector.newBuilder().setJwtProjector(JwtProjector.newBuilder()))
        .build();
  }

  AttributeRule buildRuleForCookie(String cookieName, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setCookieProjector(
                    CookieProjector.newBuilder()
                        .addCookieNameRules(
                            ParsedObjectKeyRule.newBuilder()
                                .setKey(cookieName)
                                .setAttributeRule(childRule))))
        .build();
  }

  AttributeRule buildRuleForFirstMatchingProjector(List<AttributeRule> attributeRules) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setFirstMatchingProjector(
                    FirstMatchingProjector.newBuilder().addAllAttributeRules(attributeRules)))
        .build();
  }

  AttributeRule buildRuleForEachMatchingProjector(List<AttributeRule> attributeRules) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setEachMatchingProjector(
                    EachMatchingProjector.newBuilder().addAllAttributeRules(attributeRules)))
        .build();
  }
}
