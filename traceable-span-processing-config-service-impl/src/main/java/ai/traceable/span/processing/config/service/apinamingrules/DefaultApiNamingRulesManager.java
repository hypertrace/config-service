package ai.traceable.span.processing.config.service.apinamingrules;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Stream.empty;

import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc.ApiSpecConfigServiceBlockingStub;
import ai.traceable.api.spec.config.service.v1.ApiSpecFilter;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.StringList;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.store.ApiNamingRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleMetadata;
import ai.traceable.span.processing.config.service.v1.ApiNamingRulesFilter;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultApiNamingRulesManager implements ApiNamingRulesManager {

  private final ApiNamingRulesConfigStore apiNamingRulesConfigStore;
  private final TimestampConverter timestampConverter;
  private final ApiSpecConfigServiceBlockingStub apiSpecConfigServiceBlockingStub;
  private final ApiNamingRulesManagerConfig apiNamingRulesManagerConfig;

  @Override
  public List<ApiNamingRuleDetails> getAllApiNamingRuleDetails(RequestContext requestContext) {
    return apiNamingRulesConfigStore.getAllRuleDetails(requestContext);
  }

  @Override
  public List<ApiNamingRuleDetails> getApiNamingRuleDetails(
      RequestContext requestContext, ApiNamingRulesFilter apiNamingRulesFilter) {
    return apiNamingRulesConfigStore.getRuleDetails(requestContext, apiNamingRulesFilter);
  }

  @Override
  public ApiNamingRuleDetails createApiNamingRule(
      RequestContext requestContext, CreateApiNamingRuleRequest request) {
    // TODO: need to handle priorities
    ApiNamingRuleCreationContext apiNamingRuleCreationContext =
        buildApiNamingRule(requestContext, request.getRuleInfo());
    ApiNamingRule newRule = apiNamingRuleCreationContext.getApiNamingRule();
    if (apiNamingRuleCreationContext.isNewRule()) {
      return buildApiNamingRuleDetails(
          this.apiNamingRulesConfigStore.upsertObject(requestContext, newRule));
    }

    // otherwise return the existing rule
    return this.getApiNamingRuleDetails(
            requestContext, ApiNamingRulesFilter.newBuilder().addIds(newRule.getId()).build())
        .get(0);
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
            .map(apiNamingRuleInfo -> buildApiNamingRule(requestContext, apiNamingRuleInfo))
            .filter(ApiNamingRuleCreationContext::isNewRule)
            .map(ApiNamingRuleCreationContext::getApiNamingRule);

    List<ApiNamingRuleInfo> apiSpecBasedNamingRulesInfo =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasApiSpecBasedConfig())
            .collect(toUnmodifiableList());
    Stream<ApiNamingRule> apiSpecBasedRuleStream =
        buildApiSpecBasedNamingRules(requestContext, apiSpecBasedNamingRulesInfo);

    Stream<ApiNamingRule> astScanBasedRuleStream =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasAstScanBasedConfig())
            .map(apiNamingRuleInfo -> buildApiNamingRule(requestContext, apiNamingRuleInfo))
            .filter(ApiNamingRuleCreationContext::isNewRule)
            .map(ApiNamingRuleCreationContext::getApiNamingRule);

    Stream<ApiNamingRule> jobBasedApiNamingRulesStream =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasJobBasedConfig())
            .map(apiNamingRuleInfo -> buildApiNamingRule(requestContext, apiNamingRuleInfo))
            .filter(ApiNamingRuleCreationContext::isNewRule)
            .map(ApiNamingRuleCreationContext::getApiNamingRule);

    List<ApiNamingRule> apiNamingRulesToCreate =
        Stream.of(
                segmentMatchingBasedRuleStream,
                apiSpecBasedRuleStream,
                astScanBasedRuleStream,
                jobBasedApiNamingRulesStream)
            .flatMap(Function.identity())
            .collect(toUnmodifiableList());
    if (apiNamingRulesToCreate.isEmpty()) {
      return emptyList();
    }

    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObjects(requestContext, apiNamingRulesToCreate));
  }

  private Stream<ApiNamingRule> buildApiSpecBasedNamingRules(
      RequestContext requestContext, List<ApiNamingRuleInfo> apiSpecBasedApiNamingRulesInfo) {
    if (apiSpecBasedApiNamingRulesInfo.isEmpty()) {
      return empty();
    }
    Map<String, List<ApiNamingRuleInfo>> specIdToApiNamingRulesInfoMap = new HashMap<>();
    apiSpecBasedApiNamingRulesInfo.forEach(
        apiNamingRuleInfo ->
            getSpecIdsFromApiNamingRuleInfo(apiNamingRuleInfo)
                .forEach(
                    specId ->
                        specIdToApiNamingRulesInfoMap
                            .computeIfAbsent(specId, key -> new ArrayList<>())
                            .add(apiNamingRuleInfo)));

    Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap =
        getAllApiNamingRuleDetails(requestContext).stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toMap(ApiNamingRule::getId, Function.identity()));

    deleteOrUpdateExistingApiNamingRules(
        requestContext,
        unmodifiableMap(existingApiNamingRuleIdToRuleMap),
        unmodifiableMap(specIdToApiNamingRulesInfoMap));

    Set<String> apiSpecBasedNamingRuleIds = new HashSet<>();
    apiSpecBasedApiNamingRulesInfo.forEach(
        apiNamingRuleInfo -> {
          ApiNamingRule apiNamingRule =
              createApiSpecBasedNamingRule(
                  unmodifiableMap(existingApiNamingRuleIdToRuleMap), apiNamingRuleInfo);
          // if the rule already exists, then don't create it again
          if (!apiNamingRule.equals(existingApiNamingRuleIdToRuleMap.get(apiNamingRule.getId()))) {
            existingApiNamingRuleIdToRuleMap.put(apiNamingRule.getId(), apiNamingRule);
            apiSpecBasedNamingRuleIds.add(apiNamingRule.getId());
          }
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
        apiNamingRulesConfigStore.getAllRuleDetails(requestContext).stream()
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

  private ApiNamingRuleCreationContext buildApiNamingRule(
      RequestContext requestContext, ApiNamingRuleInfo apiNamingRuleInfo) {
    switch (apiNamingRuleInfo.getRuleConfig().getRuleConfigCase()) {
      case SEGMENT_MATCHING_BASED_CONFIG:
      case JOB_BASED_CONFIG:
      case AST_SCAN_BASED_CONFIG:
        return ApiNamingRuleCreationContext.builder()
            .apiNamingRule(
                ApiNamingRule.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setRuleInfo(apiNamingRuleInfo)
                    .build())
            .isNewRule(true)
            .build();
      case API_SPEC_BASED_CONFIG:
        Map<String, List<ApiNamingRuleInfo>> apiSpecIdToApiNamingRuleInfo =
            getSpecIdsFromApiNamingRuleInfo(apiNamingRuleInfo).stream()
                .collect(toUnmodifiableMap(Function.identity(), key -> List.of(apiNamingRuleInfo)));

        Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap =
            getAllApiNamingRuleDetails(requestContext).stream()
                .map(ApiNamingRuleDetails::getRule)
                .collect(Collectors.toUnmodifiableMap(ApiNamingRule::getId, Function.identity()));

        deleteOrUpdateExistingApiNamingRules(
            requestContext, existingApiNamingRuleIdToRuleMap, apiSpecIdToApiNamingRuleInfo);
        ApiNamingRule apiNamingRule =
            createApiSpecBasedNamingRule(existingApiNamingRuleIdToRuleMap, apiNamingRuleInfo);
        if (!apiNamingRule.equals(existingApiNamingRuleIdToRuleMap.get(apiNamingRule.getId()))) {
          return ApiNamingRuleCreationContext.builder()
              .apiNamingRule(apiNamingRule)
              .isNewRule(true)
              .build();
        }
        return ApiNamingRuleCreationContext.builder()
            .apiNamingRule(existingApiNamingRuleIdToRuleMap.get(apiNamingRule.getId()))
            .isNewRule(false)
            .build();
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
      return ApiNamingRule.newBuilder(existingApiNamingRule)
          .setRuleInfo(
              ApiNamingRuleInfo.newBuilder(existingApiNamingRule.getRuleInfo())
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
                                          .distinct()
                                          .collect(toUnmodifiableList()))
                                  .addAllRegexes(apiSpecBasedConfig.getRegexesList())
                                  .addAllValues(apiSpecBasedConfig.getValuesList()))))
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

  private void deleteOrUpdateExistingApiNamingRules(
      RequestContext requestContext,
      Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap,
      Map<String, List<ApiNamingRuleInfo>> apiSpecIdToApiNamingRulesInfo) {
    Map<String, ApiNamingRule> apiNamingRuleIdToUpdatedRuleMap =
        new HashMap<>(existingApiNamingRuleIdToRuleMap);
    Set<String> updatedApiNamingRuleIds = new HashSet<>();
    for (Entry<String, List<ApiNamingRuleInfo>> apiSpecIdToApiNamingRuleInfo :
        apiSpecIdToApiNamingRulesInfo.entrySet()) {
      deleteOrUpdateAndReturnApiNamingRulesForSpecId(
              unmodifiableMap(apiNamingRuleIdToUpdatedRuleMap),
              apiSpecIdToApiNamingRuleInfo.getKey(),
              apiSpecIdToApiNamingRuleInfo.getValue())
          .forEach(
              apiNamingRule -> {
                apiNamingRuleIdToUpdatedRuleMap.put(apiNamingRule.getId(), apiNamingRule);
                updatedApiNamingRuleIds.add(apiNamingRule.getId());
              });
    }

    List<ApiNamingRule> specBasedNamingRulesToUpdate = new ArrayList<>();
    List<String> specBasedNamingRuleIdsToDelete = new ArrayList<>();
    List<String> specIdsInUpdatedRules =
        updatedApiNamingRuleIds.stream()
            .map(apiNamingRuleIdToUpdatedRuleMap::get)
            .flatMap(
                apiNamingRule ->
                    getSpecIdsFromApiNamingRuleInfo(apiNamingRule.getRuleInfo()).stream())
            .distinct()
            .collect(Collectors.toUnmodifiableList());
    Map<String, Boolean> specIdsToApiNamingEnabledMap =
        getApiSpecsFromIds(requestContext, specIdsInUpdatedRules).stream()
            .collect(
                Collectors.toUnmodifiableMap(ApiSpec::getSpecId, ApiSpec::getApiNamingEnabled));
    for (String apiNamingRuleId : updatedApiNamingRuleIds) {
      ApiNamingRule apiNamingRule = apiNamingRuleIdToUpdatedRuleMap.get(apiNamingRuleId);
      if (getSpecIdsFromApiNamingRuleInfo(apiNamingRule.getRuleInfo()).isEmpty()) {
        specBasedNamingRuleIdsToDelete.add(apiNamingRuleId);
      } else {
        specBasedNamingRulesToUpdate.add(
            buildApiNamingRuleAndSetRuleDisabled(apiNamingRule, specIdsToApiNamingEnabledMap));
      }
    }
    if (!specBasedNamingRulesToUpdate.isEmpty()) {
      this.apiNamingRulesConfigStore.upsertObjects(requestContext, specBasedNamingRulesToUpdate);
    }
    if (!specBasedNamingRuleIdsToDelete.isEmpty()) {
      this.apiNamingRulesConfigStore.deleteObjects(requestContext, specBasedNamingRuleIdsToDelete);
    }
  }

  private List<ApiNamingRule> deleteOrUpdateAndReturnApiNamingRulesForSpecId(
      Map<String, ApiNamingRule> existingApiNamingRuleIdToRuleMap,
      String apiSpecId,
      List<ApiNamingRuleInfo> apiNamingRulesInfo) {
    // Ids of existing API Naming Rules with API Spec based configs, containing given ApiSpecId
    List<String> existingSpecBasedNamingRuleIdsToCleanUp =
        existingApiNamingRuleIdToRuleMap.values().stream()
            .filter(
                apiNamingRule ->
                    getSpecIdsFromApiNamingRuleInfo(apiNamingRule.getRuleInfo())
                        .contains(apiSpecId))
            .map(ApiNamingRule::getId)
            .collect(Collectors.toList());
    // Ids of existing API Naming Rules similar to the given Naming rule Infos that are to be added
    // which contain given ApiSpecId
    List<String> specBasedNamingRuleIdsMatchingExistingRules =
        apiNamingRulesInfo.stream()
            .map(
                apiNamingRuleInfo ->
                    checkIfApiNamingRuleAlreadyExists(
                        existingApiNamingRuleIdToRuleMap.values().stream(), apiNamingRuleInfo))
            .flatMap(Optional::stream)
            .map(ApiNamingRule::getId)
            .collect(toUnmodifiableList());
    // Filter out API Naming Rules where updated would be redundant
    existingSpecBasedNamingRuleIdsToCleanUp.removeAll(specBasedNamingRuleIdsMatchingExistingRules);

    List<ApiNamingRule> cleanedUpApiNamingRules = new ArrayList<>();
    existingSpecBasedNamingRuleIdsToCleanUp.forEach(
        apiNamingId -> {
          List<String> remainingApiSpecIds =
              new ArrayList<>(
                  getSpecIdsFromApiNamingRuleInfo(
                      existingApiNamingRuleIdToRuleMap.get(apiNamingId).getRuleInfo()));

          remainingApiSpecIds.remove(apiSpecId);
          ApiNamingRule cleanedUpApiNamingRule =
              buildCleanUpApinamingRule(
                  existingApiNamingRuleIdToRuleMap.get(apiNamingId), remainingApiSpecIds);
          cleanedUpApiNamingRules.add(cleanedUpApiNamingRule);
        });
    return unmodifiableList(cleanedUpApiNamingRules);
  }

  private List<ApiSpec> getApiSpecsFromIds(RequestContext requestContext, List<String> apiSpecIds) {
    if (apiSpecIds.isEmpty()) {
      return emptyList();
    }

    GetApiSpecsRequest request =
        GetApiSpecsRequest.newBuilder()
            .setApiSpecFilter(
                ApiSpecFilter.newBuilder().setIds(StringList.newBuilder().addAllValues(apiSpecIds)))
            .build();
    return requestContext
        .call(
            () ->
                this.apiSpecConfigServiceBlockingStub
                    .withDeadlineAfter(
                        apiNamingRulesManagerConfig.getApiSpecServiceTimeout().toMillis(),
                        MILLISECONDS)
                    .getApiSpecs(request))
        .getApiSpecsList();
  }

  private ApiNamingRule buildApiNamingRuleAndSetRuleDisabled(
      ApiNamingRule apiNamingRule, Map<String, Boolean> specIdsToApiNamingEnabledMap) {
    ApiNamingRuleInfo apiNamingRuleInfo = apiNamingRule.getRuleInfo();
    boolean ruleEnabled =
        getSpecIdsFromApiNamingRuleInfo(apiNamingRuleInfo).stream()
            .filter(specIdsToApiNamingEnabledMap::containsKey)
            .anyMatch(specIdsToApiNamingEnabledMap::get);
    return ApiNamingRule.newBuilder(apiNamingRule)
        .setRuleInfo(ApiNamingRuleInfo.newBuilder(apiNamingRuleInfo).setDisabled(!ruleEnabled))
        .build();
  }

  private ApiNamingRule buildCleanUpApinamingRule(
      ApiNamingRule apiNamingRule, List<String> remainingApiSpecIds) {
    ApiNamingRuleInfo apiNamingRuleInfo = apiNamingRule.getRuleInfo();
    ApiSpecBasedConfig apiSpecBasedConfig =
        apiNamingRuleInfo.getRuleConfig().getApiSpecBasedConfig();
    return ApiNamingRule.newBuilder(apiNamingRule)
        .setRuleInfo(
            ApiNamingRuleInfo.newBuilder(apiNamingRuleInfo)
                .setRuleConfig(
                    ApiNamingRuleConfig.newBuilder()
                        .setApiSpecBasedConfig(
                            ApiSpecBasedConfig.newBuilder(apiSpecBasedConfig)
                                .clearApiSpecIds()
                                .addAllApiSpecIds(remainingApiSpecIds))))
        .build();
  }

  private List<String> getSpecIdsFromApiNamingRuleInfo(ApiNamingRuleInfo apiNamingRuleInfo) {
    if (apiNamingRuleInfo.getRuleConfig().hasApiSpecBasedConfig()) {
      return apiNamingRuleInfo.getRuleConfig().getApiSpecBasedConfig().getApiSpecIdsList();
    }
    return emptyList();
  }

  @Value
  @Builder
  private static class ApiNamingRuleCreationContext {
    ApiNamingRule apiNamingRule;
    boolean isNewRule;
  }
}
