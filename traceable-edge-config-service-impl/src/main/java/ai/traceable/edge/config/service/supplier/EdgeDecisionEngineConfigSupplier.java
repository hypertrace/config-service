package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.supplier.actor.EdgeDecisionActorConfigSupplier;
import ai.traceable.edge.config.service.supplier.ratelimiting.EdgeDecisionRateLimitingConfigSupplier;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.validation.RequestValidator;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionEngineConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = EdgeDecisionEngineConfig.class.getSimpleName();
  private final EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;
  private final EdgeDecisionActorConfigSupplier actorConfigSupplier;
  private final EdgeDecisionRateLimitingConfigSupplier rateLimitingConfigSupplier;

  @Inject
  public EdgeDecisionEngineConfigSupplier(
      TraceableEdgeConfig config,
      EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator,
      EdgeDecisionActorConfigSupplier actorConfigSupplier,
      EdgeDecisionRateLimitingConfigSupplier rateLimitingConfigSupplier) {
    this.stub = stub;
    this.config = config;
    this.uuidGenerator = uuidGenerator;
    this.actorConfigSupplier = actorConfigSupplier;
    this.rateLimitingConfigSupplier = rateLimitingConfigSupplier;
  }

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    EdgeDecisionEngineConfig edgeDecisionEngineConfig =
        mergeConfigs(
            getStoredConfig(requestContext),
            actorConfigSupplier.getEdgeDecisionActorConfig(requestContext),
            rateLimitingConfigSupplier.getEdgeDecisionRateLimitingActorConfig(requestContext));

    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder().addConfigBytes(edgeDecisionEngineConfig.toByteString()).build();
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }

  private EdgeDecisionEngineConfig getStoredEdgeDecisionEngineConfig(
      RequestContext requestContext) {
    // get stored config. by default, get the config that's stored with the tenant id as it's id.
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            s ->
                requestContext
                    .call(
                        () ->
                            stub.withDeadlineAfter(
                                    config.getClientConfig().getTimeout().toMillis(),
                                    TimeUnit.MILLISECONDS)
                                .getEdgeDecisionEngineConfig(
                                    GetEdgeDecisionEngineConfigRequest.newBuilder()
                                        .setId(s)
                                        .build()))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }

  private List<EdgeDecisionRule> getStoredRules(RequestContext requestContext) {
    GetAllEdgeDecisionRulesResponse response =
        requestContext
            .call(
                () ->
                    stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS))
            .getAllEdgeDecisionRules(GetAllEdgeDecisionRulesRequest.getDefaultInstance());
    return response.getEdgeDecisionRulesList();
  }

  private List<EdgeDecisionSpec> getStoredSpecs(RequestContext requestContext) {
    GetAllEdgeDecisionSpecsResponse response =
        requestContext
            .call(
                () ->
                    stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS))
            .getAllEdgeDecisionSpecs(GetAllEdgeDecisionSpecsRequest.getDefaultInstance());
    return response.getEdgeDecisionSpecsList();
  }

  private EdgeDecisionEngineConfig getStoredConfig(RequestContext requestContext) {
    RequestValidator.validateRequestContext(requestContext);
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    EdgeDecisionEngineConfig decisionEngineConfig =
        getStoredEdgeDecisionEngineConfig(requestContext);
    List<EdgeDecisionRule> decisionRules = getStoredRules(requestContext);
    var decisionSpecs = getStoredSpecs(requestContext);
    List<EdgeDecisionRule> mergedRules =
        merge(decisionEngineConfig.getDecisionRulesList(), decisionRules, EdgeDecisionRule::getId);
    List<EdgeDecisionSpec> mergedSpecs =
        merge(decisionEngineConfig.getDecisionSpecsList(), decisionSpecs, EdgeDecisionSpec::getId);
    return decisionEngineConfig.toBuilder()
        .setId(tenantIdHolder.get())
        .clearDecisionRules()
        .clearDecisionSpecs()
        .addAllDecisionRules(mergedRules)
        .addAllDecisionSpecs(mergedSpecs)
        .build();
  }

  // in the current implementation, if a value is present in both list1 and list2, we pick value
  // from list2.
  // it can be enhanced to further merge.
  private static <T> List<T> merge(List<T> list1, List<T> list2, Function<T, String> idExtractor) {
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

  public static Value merge(Value value1, Value value2) {
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

  private static Value mergeStructs(Struct struct1, Struct struct2) {
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

  private static Value mergeLists(ListValue list1, ListValue list2) {
    List<Value> mergedList = new java.util.ArrayList<>(list1.getValuesList());
    mergedList.addAll(list2.getValuesList());

    return Value.newBuilder()
        .setListValue(ListValue.newBuilder().addAllValues(mergedList).build())
        .build();
  }

  private EdgeDecisionEngineConfig merge(
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

  private EdgeDecisionEngineConfig mergeConfigs(EdgeDecisionEngineConfig... decisionEngineConfigs) {
    EdgeDecisionEngineConfig config = decisionEngineConfigs[0];
    for (int ii = 1; ii < decisionEngineConfigs.length; ii++) {
      config = merge(config, decisionEngineConfigs[ii]);
    }
    return config;
  }
}
