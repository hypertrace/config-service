package ai.traceable.edge.decision.config.service.supplier;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class EdgeDecisionEngineConfigMergeUtil {
  private EdgeDecisionEngineConfigMergeUtil() {}

  // in the current implementation, if a value is present in both list1 and list2, we pick value
  // from list2.
  // it can be enhanced to further merge.
  static <T> List<T> merge(List<T> list1, List<T> list2, Function<T, String> idExtractor) {
    List<T> merged = new ArrayList<>();

    // Create maps using the provided idExtractor
    Map<String, T> map1 =
        list1.stream().collect(Collectors.toMap(idExtractor, Function.identity()));
    Map<String, T> map2 =
        list2.stream().collect(Collectors.toMap(idExtractor, Function.identity()));

    // Merge the first map with the second
    for (var entry : map1.entrySet()) {
      var key = entry.getKey();
      var value = map2.getOrDefault(key, entry.getValue());
      merged.add(value);
      map2.remove(key);
    }

    // Add remaining entries from the second map
    merged.addAll(map2.values());

    return merged;
  }

  static Value merge(Value value1, Value value2) {
    if (value1 == null || value1.getKindCase() == Value.KindCase.NULL_VALUE) {
      return value2;
    }
    if (value2 == null || value2.getKindCase() == Value.KindCase.NULL_VALUE) {
      return value1;
    }

    switch (value1.getKindCase()) {
      case STRUCT_VALUE:
        if (value2.getKindCase() == Value.KindCase.STRUCT_VALUE) {
          return mergeStructs(value1.getStructValue(), value2.getStructValue());
        }
        break;

      case LIST_VALUE:
        if (value2.getKindCase() == Value.KindCase.LIST_VALUE) {
          return mergeLists(value1.getListValue(), value2.getListValue());
        }
        break;

      case NUMBER_VALUE:
      case STRING_VALUE:
      case BOOL_VALUE:
        // For primitives, prefer the second value
        return value2;

      default:
        break;
    }

    // Default: prefer value2 if types are incompatible
    return value2;
  }

  static Value mergeStructs(Struct struct1, Struct struct2) {
    Map<String, Value> mergedMap = new HashMap<>(struct1.getFieldsMap());

    struct2
        .getFieldsMap()
        .forEach(
            (key, value2) -> {
              Value value1 = mergedMap.get(key);
              mergedMap.put(key, merge(value1, value2));
            });

    return Value.newBuilder()
        .setStructValue(Struct.newBuilder().putAllFields(mergedMap).build())
        .build();
  }

  static Value mergeLists(ListValue list1, ListValue list2) {
    List<Value> mergedList = new ArrayList<>(list1.getValuesList());
    mergedList.addAll(list2.getValuesList());

    return Value.newBuilder()
        .setListValue(ListValue.newBuilder().addAllValues(mergedList).build())
        .build();
  }

  static EdgeDecisionEngineConfig merge(
      EdgeDecisionEngineConfig config1, EdgeDecisionEngineConfig config2) {
    if (config1 == null || config1.getDisabled()) {
      return config2;
    }
    if (config2 == null || config2.getDisabled()) {
      return config1;
    }
    EdgeDecisionEngineConfig.Builder merged = config1.toBuilder();
    merged.setId(config1.getId() + ":" + config2.getId());
    merged.setName(config1.getName() + ":" + config2.getName());
    merged.setVersion(Math.max(config1.getVersion(), config2.getVersion()));
    merged.addAllCommonVariables(
        merge(
            config1.getCommonVariablesList(),
            config2.getCommonVariablesList(),
            VariableDerivationMapping::getName));
    merged.addAllDecisionRules(
        merge(
            config1.getDecisionRulesList(),
            config2.getDecisionRulesList(),
            EdgeDecisionRule::getId));
    merged.addAllDecisionSpecs(
        merge(
            config1.getDecisionSpecsList(),
            config2.getDecisionSpecsList(),
            EdgeDecisionSpec::getId));
    merged.setDisabled(false);
    merged.setCustomConfig(merge(config1.getCustomConfig(), config2.getCustomConfig()));
    return merged.build();
  }
}
