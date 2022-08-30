package org.hypertrace.span.processing.config.service;

import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.apinamingrules.ApiNamingRulesManager;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;

@Slf4j
class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {

  private static final String SPAN_PROCESSING_CONFIG_SERVICE = "span.processing.config.service";
  private static final String SYSTEM_LEVEL_EXCLUDE_SPAN_RULES = "system.level.exclude.span.rules";

  private final SpanProcessingConfigRequestValidator validator;
  private final ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore;
  private final TimestampConverter timestampConverter;
  private final ApiNamingRulesManager apiNamingRulesManager;
  private List<ExcludeSpanRule> systemLevelExcludeSpanRules;
  private Map<String, ExcludeSpanRuleDetails> systemLevelExcludeSpanRulesToIdMap;

  @Inject
  SpanProcessingConfigServiceImpl(
      ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore,
      SpanProcessingConfigRequestValidator requestValidator,
      TimestampConverter timestampConverter,
      ApiNamingRulesManager apiNamingRulesManager,
      Config config) {
    this.validator = requestValidator;
    this.excludeSpanRulesConfigStore = excludeSpanRulesConfigStore;
    this.timestampConverter = timestampConverter;
    this.apiNamingRulesManager = apiNamingRulesManager;

    buildSystemLevelExcludeSpanRuleConfigs(config);
  }

  private void buildSystemLevelExcludeSpanRuleConfigs(Config config) {
    List<? extends com.typesafe.config.ConfigObject> systemLevelExcludeSpanRuleObjectList = null;
    if (config.hasPath(SPAN_PROCESSING_CONFIG_SERVICE)) {
      Config spanProcessingConfig = config.getConfig(SPAN_PROCESSING_CONFIG_SERVICE);
      if (spanProcessingConfig.hasPath(SYSTEM_LEVEL_EXCLUDE_SPAN_RULES)) {
        systemLevelExcludeSpanRuleObjectList =
            spanProcessingConfig.getObjectList(SYSTEM_LEVEL_EXCLUDE_SPAN_RULES);
      }
    }
    if (systemLevelExcludeSpanRuleObjectList != null) {
      systemLevelExcludeSpanRules =
          buildSystemLevelExcludeSpanRules(systemLevelExcludeSpanRuleObjectList);
      systemLevelExcludeSpanRulesToIdMap = buildSystemLevelExcludeSpanRulesToIdMap();
    } else {
      systemLevelExcludeSpanRules = Collections.emptyList();
      systemLevelExcludeSpanRulesToIdMap = Collections.emptyMap();
    }
  }

  @Override
  public void getAllExcludeSpanRules(
      GetAllExcludeSpanRulesRequest request,
      StreamObserver<GetAllExcludeSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      List<ExcludeSpanRuleDetails> excludeSpanRules =
          excludeSpanRulesConfigStore.getAllData(requestContext);

      Map<String, ExcludeSpanRuleDetails> tenantLevelExcludeSpanRulesToIdMap =
          buildTenantLevelExcludeSpanRulesToIdMap(excludeSpanRules);

      // all the tenant level configs except for the overridden system level ones
      List<ExcludeSpanRuleDetails> filteredTenantLevelExcludeSpanRules =
          excludeSpanRules.stream()
              .filter(
                  excludeSpanRule ->
                      !systemLevelExcludeSpanRulesToIdMap.containsKey(
                          excludeSpanRule.getRule().getId()))
              .collect(Collectors.toUnmodifiableList());

      // all the system level configs, replaced by the overridden configs in case of overrides
      List<ExcludeSpanRuleDetails> filteredSystemLevelExcludeSpanRules =
          systemLevelExcludeSpanRules.stream()
              .map(
                  excludeSpanRule ->
                      tenantLevelExcludeSpanRulesToIdMap.containsKey(excludeSpanRule.getId())
                          ? tenantLevelExcludeSpanRulesToIdMap.get(excludeSpanRule.getId())
                          : ExcludeSpanRuleDetails.newBuilder().setRule(excludeSpanRule).build())
              .collect(Collectors.toUnmodifiableList());

      responseObserver.onNext(
          GetAllExcludeSpanRulesResponse.newBuilder()
              .addAllRuleDetails(filteredTenantLevelExcludeSpanRules)
              .addAllRuleDetails(filteredSystemLevelExcludeSpanRules)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all exclude span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createExcludeSpanRule(
      CreateExcludeSpanRuleRequest request,
      StreamObserver<CreateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      ExcludeSpanRule newRule =
          ExcludeSpanRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateExcludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildExcludeSpanRuleDetails(
                      this.excludeSpanRulesConfigStore.upsertObject(requestContext, newRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating exclude span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateExcludeSpanRule(
      UpdateExcludeSpanRuleRequest request,
      StreamObserver<UpdateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateExcludeSpanRule updateExcludeSpanRule = request.getRule();

      // check if the rule already exists. If not, check if it is a system level config. If yes, use
      // that. Else, error out.
      ExcludeSpanRule existingRule =
          this.excludeSpanRulesConfigStore
              .getData(requestContext, updateExcludeSpanRule.getId())
              .or(() -> getSystemLevelExcludeSpanRule(updateExcludeSpanRule.getId()))
              .orElseThrow(Status.NOT_FOUND::asException);
      ExcludeSpanRule updatedRule = buildUpdatedRule(existingRule, updateExcludeSpanRule);

      responseObserver.onNext(
          UpdateExcludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildExcludeSpanRuleDetails(
                      this.excludeSpanRulesConfigStore.upsertObject(requestContext, updatedRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteExcludeSpanRule(
      DeleteExcludeSpanRuleRequest request,
      StreamObserver<DeleteExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.excludeSpanRulesConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteExcludeSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private List<ExcludeSpanRule> buildSystemLevelExcludeSpanRules(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(this::buildExcludeSpanRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private ExcludeSpanRule buildExcludeSpanRuleFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    ExcludeSpanRule.Builder builder = ExcludeSpanRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  private Optional<ExcludeSpanRule> getSystemLevelExcludeSpanRule(String id) {
    if (systemLevelExcludeSpanRulesToIdMap.containsKey(id)) {
      return Optional.of(systemLevelExcludeSpanRulesToIdMap.get(id).getRule());
    }
    return Optional.empty();
  }

  private Map<String, ExcludeSpanRuleDetails> buildSystemLevelExcludeSpanRulesToIdMap() {
    return systemLevelExcludeSpanRules.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ExcludeSpanRule::getId,
                (ExcludeSpanRule excludeSpanRule) ->
                    ExcludeSpanRuleDetails.newBuilder().setRule(excludeSpanRule).build()));
  }

  private Map<String, ExcludeSpanRuleDetails> buildTenantLevelExcludeSpanRulesToIdMap(
      List<ExcludeSpanRuleDetails> excludeSpanRules) {
    return excludeSpanRules.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                (ExcludeSpanRuleDetails ruleDetails) -> ruleDetails.getRule().getId(),
                Function.identity()));
  }

  private ExcludeSpanRule buildUpdatedRule(
      ExcludeSpanRule existingRule, UpdateExcludeSpanRule updateExcludeSpanRule) {
    return ExcludeSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            ExcludeSpanRuleInfo.newBuilder()
                .setName(updateExcludeSpanRule.getName())
                .setFilter(updateExcludeSpanRule.getFilter())
                .setDisabled(updateExcludeSpanRule.getDisabled())
                .setType(existingRule.getRuleInfo().getType())
                .build())
        .build();
  }

  private ExcludeSpanRuleDetails buildExcludeSpanRuleDetails(
      ContextualConfigObject<ExcludeSpanRule> configObject) {
    return ExcludeSpanRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ExcludeSpanRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  @Override
  public void getAllApiNamingRules(
      GetAllApiNamingRulesRequest request,
      StreamObserver<GetAllApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllApiNamingRulesResponse.newBuilder()
              .addAllRuleDetails(apiNamingRulesManager.getAllApiNamingRuleDetails(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all api naming rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createApiNamingRule(
      CreateApiNamingRuleRequest request,
      StreamObserver<CreateApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          CreateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(apiNamingRulesManager.createApiNamingRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating api naming rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createApiNamingRules(
      CreateApiNamingRulesRequest request,
      StreamObserver<CreateApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          CreateApiNamingRulesResponse.newBuilder()
              .addAllRulesDetails(
                  apiNamingRulesManager.createApiNamingRules(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating api naming rules {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateApiNamingRule(
      UpdateApiNamingRuleRequest request,
      StreamObserver<UpdateApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          UpdateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(apiNamingRulesManager.updateApiNamingRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateApiNamingRules(
      UpdateApiNamingRulesRequest request,
      StreamObserver<UpdateApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          UpdateApiNamingRulesResponse.newBuilder()
              .addAllRulesDetails(
                  apiNamingRulesManager.updateApiNamingRules(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api naming rules: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiNamingRule(
      DeleteApiNamingRuleRequest request,
      StreamObserver<DeleteApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      apiNamingRulesManager.deleteApiNamingRule(requestContext, request);

      responseObserver.onNext(DeleteApiNamingRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiNamingRules(
      DeleteApiNamingRulesRequest request,
      StreamObserver<DeleteApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      apiNamingRulesManager.deleteApiNamingRules(requestContext, request);

      responseObserver.onNext(DeleteApiNamingRulesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api naming rules: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
