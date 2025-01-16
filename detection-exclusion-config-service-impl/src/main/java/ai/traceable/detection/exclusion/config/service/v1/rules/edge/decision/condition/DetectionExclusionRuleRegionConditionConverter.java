package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleRegionConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  private static final String COUNTRY_ISO_CODE_JEXL_EXP =
      "$s.getIpIntelligenceData().getCountry().getIsoCode()";
  private static final AttributeDerivationMapping COUNTRY_ISO_CODE_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName("lhs")
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(COUNTRY_ISO_CODE_JEXL_EXP))))
          .build();

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    RegionCondition regionCondition = condition.getRegionCondition();
    List<String> countryIsoCodes =
        regionCondition.getRegionsList().stream()
            .map(RegionCondition.Region::getCountryIsoCode)
            .collect(Collectors.toUnmodifiableList());
    return JexlUtils.buildInOperatorMatchCondition(COUNTRY_ISO_CODE_ATTRIBUTE, countryIsoCodes)
        .setNegate(regionCondition.getExclude())
        .build();
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.REGION_CONDITION;
  }
}
