package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.Condition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.IdType;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.DataTypeConditionConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataTypeRuleWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;

@Value
@Getter
public class EnrichedRateLimitingModsecRule {
  String id;
  RateLimitingModsecRule rule;
  List<String> serviceNames = new ArrayList<>();
  List<String> urlRegexes = new ArrayList<>();
  List<KeyValueCondition> keyValueConditions = new ArrayList<>();
  List<DataTypeRuleWrapper> dataTypeRuleWrappers = new ArrayList<>();

  EnrichedRateLimitingModsecRule(
      final RateLimitingRule rule,
      DataClassificationInfo dataClassificationInfo,
      Function<String, String> serviceNameProvider) {
    this.id = rule.getId();
    // For now working with a single layer of composite condition
    // Operator is AND
    if (rule.getData().getCondition().hasCompositeCondition()) {
      rule.getData()
          .getCondition()
          .getCompositeCondition()
          .getChildrenList()
          .forEach(
              condition ->
                  this.handleLeafCondition(condition, dataClassificationInfo, serviceNameProvider));
    } else {
      handleLeafCondition(
          rule.getData().getCondition(), dataClassificationInfo, serviceNameProvider);
    }

    this.rule =
        RateLimitingModsecRule.newBuilder()
            .setId(rule.getId())
            .setData(rule.getData())
            .addAssociatedModsecRuleIds(
                ModsecRuleIdInfo.newBuilder()
                    .addMatchingIds(rule.getId())
                    .setType(IdType.ID_TYPE_KEY_VALUE_CONDITION_URL_REGEXES))
            .addAssociatedModsecRuleIds(
                ModsecRuleIdInfo.newBuilder()
                    .addAllMatchingIds(
                        dataTypeRuleWrappers.stream()
                            .map(DataTypeRuleWrapper::getModsecRuleId)
                            .collect(Collectors.toUnmodifiableList()))
                    .setType(IdType.ID_TYPE_DATA_TYPE_CUSTOM_LOCATION))
            .build();
  }

  private void handleLeafCondition(
      Condition condition,
      DataClassificationInfo dataClassificationInfo,
      Function<String, String> serviceNameProvider) {
    if (condition.getConditionCase().equals(ConditionCase.COMPOSITE_CONDITION)) {
      throw new UnsupportedOperationException(
          "Cannot convert nested composite conditions to modsec blob");
    }
    switch (condition.getLeafCondition().getConditionCase()) {
      case SCOPE_CONDITION:
        if (condition.getLeafCondition().getScopeCondition().hasUrlScope()) {
          urlRegexes.addAll(
              condition.getLeafCondition().getScopeCondition().getUrlScope().getUrlRegexesList());
        } else if (condition
            .getLeafCondition()
            .getScopeCondition()
            .getEntityScope()
            .getEntityType()
            .equals(EntityType.ENTITY_TYPE_SERVICE)) {
          serviceNames.addAll(
              condition
                  .getLeafCondition()
                  .getScopeCondition()
                  .getEntityScope()
                  .getEntityIdsList()
                  .stream()
                  .map(serviceNameProvider)
                  .collect(Collectors.toUnmodifiableList()));
        } else {
          // TODO handle service label
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot handle scope conditions - %s while converting to modsec",
                  condition.getLeafCondition().getScopeCondition()));
        }
        break;
      case KEY_VALUE_CONDITION:
        keyValueConditions.add(condition.getLeafCondition().getKeyValueCondition());
        break;
      case DATATYPE_CONDITION:
        dataTypeRuleWrappers.addAll(
            DataTypeConditionConverter.converter(
                condition.getLeafCondition().getDatatypeCondition(), dataClassificationInfo));
        break;
      case REGION_CONDITION:
      case IP_ADDRESS_CONDITION:
      case IP_LOCATION_TYPE_CONDITION:
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot convert condition of type %s",
                condition.getLeafCondition().getConditionCase()));
    }
  }
}
