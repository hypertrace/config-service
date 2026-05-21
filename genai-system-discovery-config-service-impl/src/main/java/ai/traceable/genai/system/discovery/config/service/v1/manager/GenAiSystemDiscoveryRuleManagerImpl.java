package ai.traceable.genai.system.discovery.config.service.v1.manager;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.config.GenAiSystemDiscoveryConfig;
import ai.traceable.genai.system.discovery.config.service.v1.store.GenAiSystemDiscoveryRuleStore;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class GenAiSystemDiscoveryRuleManagerImpl implements GenAiSystemDiscoveryRuleManager {
  private final UuidGenerator uuidGenerator;
  private final GenAiSystemDiscoveryRuleStore genAiSystemDiscoveryRuleStore;
  private final GenAiSystemDiscoveryConfig config;

  @Inject
  public GenAiSystemDiscoveryRuleManagerImpl(
      UuidGenerator uuidGenerator,
      GenAiSystemDiscoveryRuleStore genAiSystemDiscoveryRuleStore,
      GenAiSystemDiscoveryConfig config) {

    this.genAiSystemDiscoveryRuleStore = genAiSystemDiscoveryRuleStore;
    this.uuidGenerator = uuidGenerator;
    this.config = config;
  }

  @Override
  public List<GenAiSystemDiscoveryRule> getGenAiSystemDiscoveryRules(
      RequestContext requestContext, GetGenAiSystemDiscoveryRulesFilter filter) {
    return mergeGenAiSystemDiscoveryRules(
        requestContext,
        filter,
        genAiSystemDiscoveryRuleStore.getAllConfigData(requestContext, filter));
  }

  @Override
  public GenAiSystemDiscoveryRule createGenAiSystemDiscoveryRule(
      RequestContext requestContext, CreateGenAiSystemDiscoveryRuleRequest createRuleRequest) {
    String ruleId = this.uuidGenerator.generateId(UUID.randomUUID().toString());
    GenAiSystemDiscoveryRule genAiSystemDiscoveryRule =
        GenAiSystemDiscoveryRule.newBuilder()
            .setRuleId(ruleId)
            .setGenAiSystemDiscoveryRuleData(createRuleRequest.getGenAiSystemDiscoveryRuleData())
            .build();
    return upsertObject(requestContext, genAiSystemDiscoveryRule);
  }

  @Override
  public GenAiSystemDiscoveryRule updateGenAiSystemDiscoveryRule(
      RequestContext requestContext, UpdateGenAiSystemDiscoveryRuleRequest updateRuleRequest) {
    String ruleId = updateRuleRequest.getGenAiSystemDiscoveryRule().getRuleId();
    GenAiSystemDiscoveryRule oldRule =
        genAiSystemDiscoveryRuleStore
            .getData(requestContext, ruleId)
            .orElseGet(
                () ->
                    Optional.ofNullable(
                            config
                                .getDefaultGenAiSystemDiscoveryRuleMap(requestContext)
                                .get(ruleId))
                        .orElseThrow(
                            () ->
                                Status.NOT_FOUND
                                    .withDescription(
                                        String.format(
                                            "Unable to update as GenAiSystemDiscoveryRule with id = %s does not exist",
                                            ruleId))
                                    .asRuntimeException()));
    GenAiSystemDiscoveryRule updateRule =
        oldRule.toBuilder()
            .setGenAiSystemDiscoveryRuleData(
                updateRuleRequest.getGenAiSystemDiscoveryRule().getGenAiSystemDiscoveryRuleData())
            .build();
    return upsertObject(requestContext, updateRule);
  }

  @Override
  public void deleteGenAiSystemDiscoveryRule(RequestContext requestContext, String id) {
    if (config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext).containsKey(id)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Deleting default genai system discovery rule is not allowed with ruleID : %s",
                  id))
          .asRuntimeException();
    }
    genAiSystemDiscoveryRuleStore
        .deleteObject(requestContext, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private GenAiSystemDiscoveryRule upsertObject(
      RequestContext requestContext, GenAiSystemDiscoveryRule genAiSystemDiscoveryRule) {
    return genAiSystemDiscoveryRuleStore
        .upsertObject(requestContext, genAiSystemDiscoveryRule)
        .getData();
  }

  private List<GenAiSystemDiscoveryRule> mergeGenAiSystemDiscoveryRules(
      RequestContext requestContext, List<GenAiSystemDiscoveryRule> genAiSystemDiscoveryRules) {
    Map<String, GenAiSystemDiscoveryRule> mergedRulesMap =
        new LinkedHashMap<>(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext));
    genAiSystemDiscoveryRules.forEach(rule -> mergedRulesMap.put(rule.getRuleId(), rule));
    return mergedRulesMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private List<GenAiSystemDiscoveryRule> mergeGenAiSystemDiscoveryRules(
      RequestContext requestContext,
      GetGenAiSystemDiscoveryRulesFilter filter,
      List<GenAiSystemDiscoveryRule> rules) {
    return mergeGenAiSystemDiscoveryRules(requestContext, rules).stream()
        .filter(rule -> filter.getEnabled() == rule.getGenAiSystemDiscoveryRuleData().getEnabled())
        .collect(Collectors.toUnmodifiableList());
  }
}
