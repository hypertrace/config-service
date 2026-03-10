package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
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
    if (regionExpression.getRegionIdentifiersList().isEmpty()) {
      throw new IllegalArgumentException("No region identifiers present in region expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_REGION);
    List<String> countryIsoCodes = new ArrayList<>();
    for (RegionExpression.Region region : regionExpression.getRegionIdentifiersList()) {
      if (region.getRegionCase() == RegionExpression.Region.RegionCase.COUNTRY_ISO_CODE) {
        countryIsoCodes.add(region.getCountryIsoCode());
      } else {
        throw new IllegalArgumentException("Unsupported region type: " + region.getRegionCase());
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
