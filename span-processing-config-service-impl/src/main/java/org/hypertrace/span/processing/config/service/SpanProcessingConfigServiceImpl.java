package org.hypertrace.span.processing.config.service;

import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_SYSTEM;

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

  private static final String SYSTEM_EXCLUDE_SPAN_RULES =
      "span.processing.config.service.system.exclude.span.rules";

  private final SpanProcessingConfigRequestValidator validator;
  private final ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore;
  private final TimestampConverter timestampConverter;
  private final ApiNamingRulesManager apiNamingRulesManager;
  private Map<String, ExcludeSpanRule> systemExcludeSpanRuleIdToRuleMap;

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

    buildSystemExcludeSpanRuleConfigs(config);
  }

  private void buildSystemExcludeSpanRuleConfigs(Config config) {
    List<? extends com.typesafe.config.ConfigObject> systemExcludeSpanRuleObjectList = null;
    if (config.hasPath(SYSTEM_EXCLUDE_SPAN_RULES)) {
      systemExcludeSpanRuleObjectList = config.getObjectList(SYSTEM_EXCLUDE_SPAN_RULES);
    }
    if (systemExcludeSpanRuleObjectList != null) {
      systemExcludeSpanRuleIdToRuleMap =
          buildSystemExcludeSpanRulesToIdMap(systemExcludeSpanRuleObjectList);
    } else {
      systemExcludeSpanRuleIdToRuleMap = Collections.emptyMap();
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

      Map<String, ExcludeSpanRuleDetails> userExcludeSpanRuleIdToRuleMap =
          buildUserExcludeSpanRulesToIdMap(excludeSpanRules);

      // all the user configs except for the overridden system ones
      List<ExcludeSpanRuleDetails> filteredUserExcludeSpanRules =
          excludeSpanRules.stream()
              .filter(
                  excludeSpanRule ->
                      !systemExcludeSpanRuleIdToRuleMap.containsKey(
                          excludeSpanRule.getRule().getId()))
              .collect(Collectors.toUnmodifiableList());

      // all the system configs, replaced by the overridden configs in case of overrides
      List<ExcludeSpanRuleDetails> filteredSystemExcludeSpanRules =
          systemExcludeSpanRuleIdToRuleMap.values().stream()
              .map(
                  excludeSpanRule ->
                      userExcludeSpanRuleIdToRuleMap.containsKey(excludeSpanRule.getId())
                          ? userExcludeSpanRuleIdToRuleMap.get(excludeSpanRule.getId())
                          : ExcludeSpanRuleDetails.newBuilder().setRule(excludeSpanRule).build())
              .collect(Collectors.toUnmodifiableList());

      responseObserver.onNext(
          GetAllExcludeSpanRulesResponse.newBuilder()
              .addAllRuleDetails(filteredUserExcludeSpanRules)
              .addAllRuleDetails(filteredSystemExcludeSpanRules)
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

      // check if the rule already exists. If not, check if it is a system config. If yes, use
      // that. Else, error out.
      ExcludeSpanRule existingRule =
          this.excludeSpanRulesConfigStore
              .getData(requestContext, updateExcludeSpanRule.getId())
              .or(() -> getSystemExcludeSpanRule(updateExcludeSpanRule.getId()))
              .orElseThrow(Status.NOT_FOUND::asException);

      // if the rule being updated is a system exclude rule, only disabled field update should be
      // allowed
      validateUpdateExcludeSpanRule(existingRule, updateExcludeSpanRule);
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

      // do not allow deleting system exclude rules
      if (systemExcludeSpanRuleIdToRuleMap.containsKey(request.getId())) {
        throw Status.INVALID_ARGUMENT.asRuntimeException();
      }

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

  @SneakyThrows
  private ExcludeSpanRule buildExcludeSpanRuleFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    ExcludeSpanRule.Builder builder = ExcludeSpanRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  private Optional<ExcludeSpanRule> getSystemExcludeSpanRule(String id) {
    if (systemExcludeSpanRuleIdToRuleMap.containsKey(id)) {
      return Optional.of(systemExcludeSpanRuleIdToRuleMap.get(id));
    }
    return Optional.empty();
  }

  private Map<String, ExcludeSpanRule> buildSystemExcludeSpanRulesToIdMap(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(this::buildExcludeSpanRuleFromConfig)
        .collect(Collectors.toUnmodifiableMap(ExcludeSpanRule::getId, Function.identity()));
  }

  private Map<String, ExcludeSpanRuleDetails> buildUserExcludeSpanRulesToIdMap(
      List<ExcludeSpanRuleDetails> excludeSpanRules) {
    return excludeSpanRules.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                (ExcludeSpanRuleDetails ruleDetails) -> ruleDetails.getRule().getId(),
                Function.identity()));
  }

  private void validateUpdateExcludeSpanRule(
      ExcludeSpanRule existingRule, UpdateExcludeSpanRule updateExcludeSpanRule) {
    ExcludeSpanRuleInfo ruleInfo = existingRule.getRuleInfo();
    if (RULE_TYPE_SYSTEM.equals(ruleInfo.getType())) {
      if (!updateExcludeSpanRule.getName().equals(ruleInfo.getName())
          || !updateExcludeSpanRule.getFilter().equals(ruleInfo.getFilter())) {
        throw Status.INVALID_ARGUMENT.asRuntimeException();
      }
    }
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
