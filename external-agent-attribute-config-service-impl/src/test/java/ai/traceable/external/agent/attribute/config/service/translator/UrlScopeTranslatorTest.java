package ai.traceable.external.agent.attribute.config.service.translator;

import ai.traceable.external.agent.attribute.config.service.v1.AgentAttributeRule.AttributeRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.CustomScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.SystemWideScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.UrlScope;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UrlScopeTranslatorTest {

  private final UrlScopeTranslator urlScopeTranslator =
      new UrlScopeTranslator(new AttributeRuleBuilder());

  @Test
  void addTranslatedScopeForEmptyUrlScope() {
    AttributeRule attributeRule = AttributeRule.getDefaultInstance();
    UserAttributionRuleScope ruleScope =
        UserAttributionRuleScope.newBuilder()
            .setSystemWideScope(SystemWideScope.getDefaultInstance())
            .build();
    Assertions.assertEquals(
        attributeRule, urlScopeTranslator.addUrlScopeIfSet(ruleScope, attributeRule));
  }

  @Test
  void addTranslatedScopeForSingleUrlScope() throws IOException {
    AttributeRule attributeRule = AttributeRule.getDefaultInstance();
    UserAttributionRuleScope ruleScope =
        UserAttributionRuleScope.newBuilder()
            .setCustomScope(
                CustomScope.newBuilder()
                    .addUrlScopes(
                        UrlScope.newBuilder().setUrlMatchRegex(".*/my-url-suffix").build()))
            .build();
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRule("scope/rule_for_single_url_scope.json"),
        urlScopeTranslator.addUrlScopeIfSet(ruleScope, attributeRule));
  }

  @Test
  void addTranslatedScopeForMultipleUrlScopes() throws IOException {
    AttributeRule attributeRule = AttributeRule.getDefaultInstance();
    UserAttributionRuleScope ruleScope =
        UserAttributionRuleScope.newBuilder()
            .setCustomScope(
                CustomScope.newBuilder()
                    .addUrlScopes(
                        UrlScope.newBuilder().setUrlMatchRegex(".*/my-url-suffix").build())
                    .addUrlScopes(
                        UrlScope.newBuilder().setUrlMatchRegex("my-url-prefix/.*").build()))
            .build();
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRule("scope/rule_for_multiple_url_scopes.json"),
        urlScopeTranslator.addUrlScopeIfSet(ruleScope, attributeRule));
  }
}
