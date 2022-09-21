package org.hypertrace.span.processing.config.service.apinamingrules;

import static org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.API_SPEC_BASED_CONFIG;
import static org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig.RuleConfigCase.SEGMENT_MATCHING_BASED_CONFIG;

import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.ApiSpecBasedConfig;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRulesRequest;

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

    Stream<ApiNamingRuleInfo> apiSpecBasedNamingRulesInfo =
        request.getRulesInfoList().stream()
            .filter(apiNamingRuleInfo -> apiNamingRuleInfo.getRuleConfig().hasApiSpecBasedConfig());
    Stream<ApiNamingRule> apiSpecBasedRuleStream =
        buildApiSpecBasedNamingRules(requestContext, apiSpecBasedNamingRulesInfo);

    return buildApiNamingRuleDetails(
        this.apiNamingRulesConfigStore.upsertObjects(
            requestContext,
            Stream.concat(segmentMatchingBasedRuleStream, apiSpecBasedRuleStream)
                .collect(Collectors.toUnmodifiableList())));
  }

  private Stream<ApiNamingRule> buildApiSpecBasedNamingRules(
      RequestContext requestContext,
      Stream<ApiNamingRuleInfo> apiSpecBasedApiNamingRuleInfoStream) {
    List<ApiNamingRule> existingApiNamingRules =
        getAllApiNamingRuleDetails(requestContext).stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    return apiSpecBasedApiNamingRuleInfoStream.map(
        apiNamingRuleInfo ->
            createApiSpecBasedNamingRule(existingApiNamingRules.stream(), apiNamingRuleInfo));
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
            .collect(Collectors.toUnmodifiableList());

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
    for (String id : request.getIdsList()) {
      this.apiNamingRulesConfigStore
          .deleteObject(requestContext, id)
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    }
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
        .collect(Collectors.toUnmodifiableList());
  }

  private ApiNamingRule buildApiNamingRule(
      RequestContext requestContext, ApiNamingRuleInfo apiNamingRuleInfo) {
    switch (apiNamingRuleInfo.getRuleConfig().getRuleConfigCase()) {
      case SEGMENT_MATCHING_BASED_CONFIG:
        return ApiNamingRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setRuleInfo(apiNamingRuleInfo)
            .build();
      case API_SPEC_BASED_CONFIG:
        Stream<ApiNamingRule> existingApiNamingRuleStream =
            getAllApiNamingRuleDetails(requestContext).stream().map(ApiNamingRuleDetails::getRule);
        return createApiSpecBasedNamingRule(existingApiNamingRuleStream, apiNamingRuleInfo);
      default:
        log.error("Unrecognized api naming rule config type:{}", apiNamingRuleInfo);
        throw new RuntimeException();
    }
  }

  private ApiNamingRule createApiSpecBasedNamingRule(
      Stream<ApiNamingRule> existingApiNamingRuleStream, ApiNamingRuleInfo apiNamingRuleInfo) {
    ApiSpecBasedConfig apiSpecBasedConfig =
        apiNamingRuleInfo.getRuleConfig().getApiSpecBasedConfig();
    Optional<ApiNamingRule> apiNamingRuleMaybe =
        checkIfApiNamingRuleAlreadyExists(existingApiNamingRuleStream, apiNamingRuleInfo);
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
                                          .collect(Collectors.toUnmodifiableList()))
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
