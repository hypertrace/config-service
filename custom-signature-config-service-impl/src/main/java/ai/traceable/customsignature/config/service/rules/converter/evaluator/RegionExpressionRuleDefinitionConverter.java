package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RegionExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    RegionExpression regionExpression = clause.getRegionExpression();
    if (regionExpression.getRegionsList().isEmpty()) {
      throw new IllegalArgumentException("No region identifiers present in region expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_REGION);
    // TODO(AAP-11983): Support state/city region matching for edge conversion.
    List<String> countryIsoCodes = new ArrayList<>();
    for (RegionIdentifier regionIdentifier : regionExpression.getRegionsList()) {
      if (regionIdentifier.hasCountry() && !regionIdentifier.getCountry().getIsoCode().isEmpty()) {
        countryIsoCodes.add(regionIdentifier.getCountry().getIsoCode());
      }
    }

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
            keyPrefix, countryIsoCodes);

    if (regionExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier = "region-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.REGION_EXPRESSION;
  }
}
