package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.blocking.config.service.v2.AttributeScope;
import ai.traceable.blocking.config.service.v2.InlineModification;
import ai.traceable.blocking.config.service.v2.RuleAction;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class CustomSignatureRuleEffectConverterTest {

  @Test
  public void testConvert() {
    // Prepare the test data
    FieldValue fieldValue = FieldValue.newBuilder().setStaticValue("static-value").build();
    HeaderInjection headerInjection =
        HeaderInjection.newBuilder()
            .setHeaderCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
            .setHeaderName("Test-Header")
            .setValue(fieldValue)
            .build();
    AgentModification agentModification =
        AgentModification.newBuilder().setHeaderInjection(headerInjection).build();
    AgentRuleEffect agentRuleEffect =
        AgentRuleEffect.newBuilder().addAgentModifications(agentModification).build();
    RuleEffectWithModifications ruleEffectWithModifications =
        RuleEffectWithModifications.newBuilder().setAgentRuleEffect(agentRuleEffect).build();

    RuleAction ruleAction =
        CustomSignatureRuleEffectConverter.convert(
            Collections.singletonList(ruleEffectWithModifications));

    // Verify the result
    assertNotNull(ruleAction);
    assertEquals(1, ruleAction.getInlineModificationsCount());

    InlineModification inlineModification = ruleAction.getInlineModifications(0);
    assertEquals(
        InlineModification.ModificationCase.HEADER_INJECTION,
        inlineModification.getModificationCase());

    ai.traceable.blocking.config.service.v2.HeaderInjection convertedHeaderInjection =
        inlineModification.getHeaderInjection();
    assertNotNull(convertedHeaderInjection);
    assertEquals(AttributeScope.ATTRIBUTE_SCOPE_REQUEST, convertedHeaderInjection.getScope());
    assertEquals("Test-Header", convertedHeaderInjection.getHeaderName());
    assertEquals("static-value", convertedHeaderInjection.getValue().getStaticValue());
  }
}
