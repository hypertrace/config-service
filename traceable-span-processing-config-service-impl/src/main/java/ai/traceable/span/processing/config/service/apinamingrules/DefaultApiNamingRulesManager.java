package ai.traceable.span.processing.config.service.apinamingrules;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static java.util.stream.Stream.empty;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.store.ApiNamingRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleMetadata;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRule;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultApiNamingRulesManager implements ApiNamingRulesManager {

  private final TimestampConverter timestampConverter;
  private final ApiNamingRulesConfigStore apiNamingRulesConfigStore;

  @Inject
  public DefaultApiNamingRulesManager(
      ApiNamingRulesConfigStore apiNamingRulesConfigStore, TimestampConverter timestampConverter) {
    this.timestampConverter = timestampConverter;
    this.apiNamingRulesConfigStore = apiNamingRulesConfigStore;
  }

  @Override
  public List<ApiNamingRuleDetails> getAllApiNamingRuleDetails(RequestContext requestContext) {
    return apiNamingRulesConfigStore.getAllData(requestContext);
  }

  @Override
  public ApiNamingRuleDetails createApiNamingRule(
      RequestContext requestContext, CreateApiNamingRuleRequest request) {
    // TODO: need to handle priorities
    ApiNamingRule newRule = buildApiNamingRule(requestContext, request.getRuleInfo());
    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObject(requestContext, newRule));
  }

  @Override
  public List<ApiNamingRuleDetails> createApiNamingRules(
      RequestContext requestContext, CreateApiNamingRulesRequest request) {
    // TODO: need to handle priorities
    Stream<ApiNamingRule> segmentMatchingBasedRuleStream =
        request.getRulesInfoList().stream()
            .filter(
                apiNamingRuleInfo ->
                    apiNamingRuleInfo.getRuleConfig().hasSegmentMatchingBasedConfig())
            .map(apiNamingRuleInfo -> buildApiNamingRule(requestContext, apiNamingRuleInfo));

    List<ApiNamingRuleInfo> apiSpecBasedNamingRulesInfo =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasApiSpecBasedConfig())
            .collect(toUnmodifiableList());
    Stream<ApiNamingRule> apiSpecBasedRuleStream =
        buildApiSpecBasedNamingRules(requestContext, apiSpecBasedNamingRulesInfo);

    Stream<ApiNamingRule> astScanBasedRuleStream =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasAstScanBasedConfig())
            .map(apiNamingRuleInfo -> buildApiNamingRule(requestContext, apiNamingRuleInfo));

    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObjects(
            requestContext,
            Stream.concat(
                    Stream.concat(segmentMatchingBasedRuleStream, apiSpecBasedRuleStream),
                    astScanBasedRuleStream)
                .collect(toUnmodifiableList())));
  }

  private Stream<ApiNamingRule> buildApiSpecBasedNamingRules(
      RequestContext requestContext, List<ApiNamingRuleInfo> apiSpecBasedApiNamingRulesInfo) {
    if (apiSpecBasedApiNamingRulesInfo.isEmpty()) {
      return empty();
    }
    List<ApiNamingRule> existingApiNamingRules =
        getAllApiNamingRuleDetails(requestContext).stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(toUnmodifiableList());

    Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap =
        existingApiNamingRules.stream()
            .collect(Collectors.toMap(ApiNamingRule::getId, Function.identity()));

    Set<String> apiSpecBasedNamingRuleIds = new HashSet<>();
    apiSpecBasedApiNamingRulesInfo.stream()
        .forEach(
            apiNamingRuleInfo -> {
              ApiNamingRule apiNamingRule =
                  createApiSpecBasedNamingRule(
                      unmodifiableMap(existingApiNamingRuleIdToRuleMap), apiNamingRuleInfo);
              existingApiNamingRuleIdToRuleMap.put(apiNamingRule.getId(), apiNamingRule);
              apiSpecBasedNamingRuleIds.add(apiNamingRule.getId());
            });

    return apiSpecBasedNamingRuleIds.stream().map(existingApiNamingRuleIdToRuleMap::get);
  }

  @Override
  public ApiNamingRuleDetails updateApiNamingRule(
      RequestContext requestContext, UpdateApiNamingRuleRequest request) throws StatusException {
    UpdateApiNamingRule updateApiNamingRule = request.getRule();
    ApiNamingRule existingRule =
        this.apiNamingRulesConfigStore
            .getData(requestContext, updateApiNamingRule.getId())
            .orElseThrow(Status.NOT_FOUND::asException);
    ApiNamingRule updatedRule = buildUpdatedRule(existingRule, updateApiNamingRule);

    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObject(requestContext, updatedRule));
  }

  @Override
  public List<ApiNamingRuleDetails> updateApiNamingRules(
      RequestContext requestContext, UpdateApiNamingRulesRequest request) {

    Map<String, UpdateApiNamingRule> apiNamingRuleMap =
        request.getRulesList().stream()
            .collect(Collectors.toUnmodifiableMap(UpdateApiNamingRule::getId, Function.identity()));

    List<ApiNamingRule> existingRules =
        apiNamingRulesConfigStore.getAllData(requestContext).stream()
            .map(ApiNamingRuleDetails::getRule)
            .filter(apiNamingRule -> apiNamingRuleMap.containsKey(apiNamingRule.getId()))
            .collect(toUnmodifiableList());

    List<ApiNamingRule> updatedRules = new ArrayList<>();
    for (ApiNamingRule existingRule : existingRules) {
      updatedRules.add(buildUpdatedRule(existingRule, apiNamingRuleMap.get(existingRule.getId())));
    }
    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObjects(requestContext, updatedRules));
  }

  @Override
  public void deleteApiNamingRule(
      RequestContext requestContext, DeleteApiNamingRuleRequest request) {
    // TODO: need to handle priorities
    this.apiNamingRulesConfigStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  @Override
  public void deleteApiNamingRules(
      RequestContext requestContext, DeleteApiNamingRulesRequest request) {
    // TODO: need to handle priorities
    this.apiNamingRulesConfigStore.deleteObjects(requestContext, request.getIdsList());
  }

  private ApiNamingRule buildUpdatedRule(
      ApiNamingRule existingRule, UpdateApiNamingRule updateApiNamingRule) {
    return ApiNamingRule.newBuilder(existingRule)
        .setRuleInfo(
            ApiNamingRuleInfo.newBuilder()
                .setName(updateApiNamingRule.getName())
                .setFilter(updateApiNamingRule.getFilter())
                .setDisabled(updateApiNamingRule.getDisabled())
                .setRuleConfig(
                    ApiNamingRuleConfig.newBuilder(updateApiNamingRule.getRuleConfig()).build())
                .build())
        .build();
  }

  private ApiNamingRuleDetails buildApiNamingRuleDetails(
      ContextualConfigObject<ApiNamingRule> configObject) {
    return ApiNamingRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ApiNamingRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private List<ApiNamingRuleDetails> buildApiNamingRuleDetails(
      List<ContextualConfigObject<ApiNamingRule>> contextualConfigObjects) {
    return contextualConfigObjects.stream()
        .map(this::buildApiNamingRuleDetails)
        .collect(toUnmodifiableList());
  }

  private ApiNamingRule buildApiNamingRule(
      RequestContext requestContext, ApiNamingRuleInfo apiNamingRuleInfo) {
    switch (apiNamingRuleInfo.getRuleConfig().getRuleConfigCase()) {
      case SEGMENT_MATCHING_BASED_CONFIG:
      case AST_SCAN_BASED_CONFIG:
        return ApiNamingRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setRuleInfo(apiNamingRuleInfo)
            .build();
      case API_SPEC_BASED_CONFIG:
        Stream<ApiNamingRule> existingApiNamingRuleStream =
            getAllApiNamingRuleDetails(requestContext).stream().map(ApiNamingRuleDetails::getRule);
        Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap =
            existingApiNamingRuleStream.collect(
                Collectors.toUnmodifiableMap(ApiNamingRule::getId, Function.identity()));
        return createApiSpecBasedNamingRule(existingApiNamingRuleIdToRuleMap, apiNamingRuleInfo);
      default:
        log.error("Unrecognized api naming rule config type:{}", apiNamingRuleInfo);
        throw new RuntimeException();
    }
  }

  private ApiNamingRule createApiSpecBasedNamingRule(
      Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap,
      ApiNamingRuleInfo apiNamingRuleInfo) {
    ApiSpecBasedConfig apiSpecBasedConfig =
        apiNamingRuleInfo.getRuleConfig().getApiSpecBasedConfig();
    Optional<ApiNamingRule> apiNamingRuleMaybe =
        checkIfApiNamingRuleAlreadyExists(
            existingApiNamingRuleIdToRuleMap.values().stream(), apiNamingRuleInfo);
    if (apiNamingRuleMaybe.isEmpty()) {
      return ApiNamingRule.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setRuleInfo(apiNamingRuleInfo)
          .build();
    } else {
      ApiNamingRule existingApiNamingRule = apiNamingRuleMaybe.get();
      ApiSpecBasedConfig existingApiSpecBasedConfig =
          existingApiNamingRule.getRuleInfo().getRuleConfig().getApiSpecBasedConfig();
      boolean ruleDisabled =
          existingApiNamingRule.getRuleInfo().getDisabled() && apiNamingRuleInfo.getDisabled();
      return ApiNamingRule.newBuilder()
          .setId(existingApiNamingRule.getId())
          .setRuleInfo(
              ApiNamingRuleInfo.newBuilder()
                  .setName(existingApiNamingRule.getRuleInfo().getName())
                  .setDisabled(ruleDisabled)
                  .setRuleConfig(
                      ApiNamingRuleConfig.newBuilder()
                          .setApiSpecBasedConfig(
                              ApiSpecBasedConfig.newBuilder()
                                  .addAllApiSpecIds(
                                      Stream.concat(
                                              existingApiSpecBasedConfig
                                                  .getApiSpecIdsList()
                                                  .stream(),
                                              apiSpecBasedConfig.getApiSpecIdsList().stream())
                                          .collect(toUnmodifiableSet()))
                                  .addAllRegexes(apiSpecBasedConfig.getRegexesList())
                                  .addAllValues(apiSpecBasedConfig.getValuesList())
                                  .build())
                          .build())
                  .setFilter(existingApiNamingRule.getRuleInfo().getFilter())
                  .build())
          .build();
    }
  }

  private Optional<ApiNamingRule> checkIfApiNamingRuleAlreadyExists(
      Stream<ApiNamingRule> existingApiNamingRuleStream, ApiNamingRuleInfo apiNamingRuleInfo) {
    return existingApiNamingRuleStream
        .filter(
            rule ->
                rule.getRuleInfo().getRuleConfig().hasApiSpecBasedConfig()
                    && rule.getRuleInfo() // TODO: remove this condition after migration of upstream
                            // services and existing configs
                            .getRuleConfig()
                            .getApiSpecBasedConfig()
                            .getApiSpecIdsCount()
                        != 0
                    && areSame(apiNamingRuleInfo, rule.getRuleInfo()))
        .findAny();
  }

  private boolean areSame(
      ApiNamingRuleInfo apiNamingRuleInfo1, ApiNamingRuleInfo apiNamingRuleInfo2) {
    ApiSpecBasedConfig apiSpecBasedConfig1 =
        apiNamingRuleInfo1.getRuleConfig().getApiSpecBasedConfig();
    ApiSpecBasedConfig apiSpecBasedConfig2 =
        apiNamingRuleInfo2.getRuleConfig().getApiSpecBasedConfig();
    return apiNamingRuleInfo1.getFilter().equals(apiNamingRuleInfo2.getFilter())
        && apiSpecBasedConfig1.getRegexesList().equals(apiSpecBasedConfig2.getRegexesList())
        && apiSpecBasedConfig1.getValuesList().equals(apiSpecBasedConfig2.getValuesList());
  }
}
