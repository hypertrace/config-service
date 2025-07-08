package ai.traceable.edge.config.service.supplier.filtering.config.context;

import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRulesFilter;
import ai.traceable.protection.rules.filtering.v1.StringList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
class ProtectionFilteringRulesFilterUtils {
  private static final String RULE_IDS_FIELD_KEY = "protection_filtering_rule_ids";
  private static final String RULE_TYPES_FIELD_KEY = "protection_filtering_rule_types";

  List<ProtectionFilteringRulesFilter> buildFilters(AgentCapabilities agentCapabilities) {
    List<ProtectionFilteringRulesFilter> filters = new ArrayList<>();
    Map<String, String> fieldsMap = agentCapabilities.getAdditionalFieldsMap();

    if (fieldsMap.containsKey(RULE_IDS_FIELD_KEY)) {
      String jsonSerializedList = fieldsMap.get(RULE_IDS_FIELD_KEY);
      StringList ruleIds = getAsStringList(JsonParser.parseString(jsonSerializedList));
      filters.add(ProtectionFilteringRulesFilter.newBuilder().setRuleIds(ruleIds).build());
    }

    if (fieldsMap.containsKey(RULE_TYPES_FIELD_KEY)) {
      String jsonSerializedList = fieldsMap.get(RULE_TYPES_FIELD_KEY);
      StringList ruleTypes = getAsStringList(JsonParser.parseString(jsonSerializedList));
      filters.add(ProtectionFilteringRulesFilter.newBuilder().setRuleTypes(ruleTypes).build());
    }

    return filters;
  }

  private static StringList getAsStringList(JsonElement element) {
    StringList.Builder builder = StringList.newBuilder();
    element.getAsJsonArray().forEach(elem -> builder.addValues(elem.getAsString()));
    return builder.build();
  }
}
