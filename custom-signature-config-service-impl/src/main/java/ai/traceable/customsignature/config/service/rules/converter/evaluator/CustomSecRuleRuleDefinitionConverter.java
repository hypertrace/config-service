package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CustomSecRule;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import java.util.UUID;

public class CustomSecRuleRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    CustomSecRule customSecRule = clause.getCustomSecRule();

    if (customSecRule == null) {
      throw new IllegalArgumentException("CustomSecRule is null in clause");
    }

    // Use the sanitized sec rule if available, otherwise fall back to input sec rule
    String secRule =
        customSecRule.getSanitisedSecRule().isEmpty()
            ? customSecRule.getInputSecRule()
            : customSecRule.getSanitisedSecRule();

    if (secRule == null || secRule.trim().isEmpty()) {
      throw new IllegalArgumentException("Sec rule is null or empty in CustomSecRule");
    }

    String evaluationIdentifier = "custom_sec_rule-" + UUID.randomUUID().toString().substring(0, 8);

    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, secRule);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.CUSTOM_SEC_RULE;
  }
}
