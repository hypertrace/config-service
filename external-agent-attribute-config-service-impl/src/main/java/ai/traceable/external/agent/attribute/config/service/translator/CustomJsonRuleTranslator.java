package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.DataCase;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomJsonRuleTranslator implements RuleTranslator {

  private static final Parser PARSER = JsonFormat.parser();

  @Override
  public DataCase getRuleDataCase() {
    return DataCase.CUSTOM_JSON_DATA;
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserId(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasUserIdRuleData()) {
      return translateRule(rule.getData().getCustomJsonData().getUserIdRuleData());
    }
    return Stream.empty();
  }

  @Override
  public Stream<AttributeRule> translateRuleForUserRole(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasRoleRuleData()) {
      return translateRule(rule.getData().getCustomJsonData().getRoleRuleData());
    }
    return Stream.empty();
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(UserAttributionRule rule) {
    if (rule.getData().getCustomJsonData().hasAuthTypeRuleData()) {
      return translateRule(rule.getData().getCustomJsonData().getAuthTypeRuleData());
    }
    return Stream.empty();
  }

  private Stream<AttributeRule> translateRule(String jsonRuleData) {
    AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
    try {
      PARSER.merge(jsonRuleData, attributeRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Invalid custom json rule data: {}", jsonRuleData);
    }
    return Stream.of(attributeRuleBuilder.build());
  }
}
