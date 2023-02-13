package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.END_USER_ID_RULE_ATTRIBUTE_KEY;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CustomJsonRuleTranslator implements UserAttributionRuleTranslator {

  private static final Parser PARSER = JsonFormat.parser();

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.CUSTOM_JSON_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasUserIdRuleData()) {
      return Stream.of(
          buildAttributeRuleWithUserIdRuleAttributeAdditionAction(
              rule, translateRule(rule.getData().getCustomJsonData().getUserIdRuleData())));
    }
    return Stream.empty();
  }

  public AttributeRule buildAttributeRuleWithUserIdRuleAttributeAdditionAction(
      UserAttributionRule userAttributionRule, AttributeRule attributeRule) {
    return AttributeRule.newBuilder(attributeRule)
        .addEndActions(
            AttributeRule.Action.newBuilder()
                .setAttributeAddition(
                    AttributeRule.Action.AttributeAddition.newBuilder()
                        .setAttributeKey(END_USER_ID_RULE_ATTRIBUTE_KEY)
                        .setValueProjectionRule(
                            AttributeRule.newBuilder()
                                .setProjector(
                                    AttributeRule.Projector.newBuilder()
                                        .setValueProjector(
                                            AttributeRule.Projector.StaticValueProjector
                                                .newBuilder()
                                                .setValue(userAttributionRule.getId())))))
                .build())
        .build();
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasRoleRuleData()) {
      return Stream.of(translateRule(rule.getData().getCustomJsonData().getRoleRuleData()));
    }
    return Stream.empty();
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasAuthTypeRuleData()) {
      return Stream.of(translateRule(rule.getData().getCustomJsonData().getAuthTypeRuleData()));
    }
    return Stream.empty();
  }

  private AttributeRule translateRule(String jsonRuleData) {
    AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
    try {
      PARSER.merge(jsonRuleData, attributeRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Invalid custom json rule data: {}", jsonRuleData);
    }
    return attributeRuleBuilder.build();
  }
}
