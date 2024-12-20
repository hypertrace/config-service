package ai.traceable.external.agent.attribute.config.service.translator;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_PATH_ATTRIBUTE_KEYS;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.UrlScope;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class UrlScopeTranslator {

  private final AttributeRuleBuilder attributeRuleBuilder;

  AttributeRule addUrlScopeIfSet(UserAttributionRuleScope ruleScope, AttributeRule translatedRule) {
    List<String> urlScopes =
        ruleScope.getCustomScope().getUrlScopesList().stream()
            .map(UrlScope::getUrlMatchRegex)
            .collect(Collectors.toUnmodifiableList());
    return attributeRuleBuilder.buildRuleForCondition(
        URL_OR_PATH_ATTRIBUTE_KEYS, urlScopes, translatedRule);
  }
}
