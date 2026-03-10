package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;

public interface CustomSignatureRuleDefinitionConverter {

  CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause);

  Clause.ClauseCase getClauseCase();
}
