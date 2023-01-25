package org.hypertrace.span.processing.config.service;

import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_SYSTEM;

import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
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
  private List<ExcludeSpanRule> systemExcludeSpanRules;
  private Map<String, ExcludeSpanRule> systemExcludeSpanRuleIdToRuleMap;

  @Inject
  SpanProcessingConfigServiceImpl(
      ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore,
      SpanProcessingConfigRequestValidator requestValidator,
      TimestampConverter timestampConverter,
      Config config) {
    this.validator = requestValidator;
    this.excludeSpanRulesConfigStore = excludeSpanRulesConfigStore;
    this.timestampConverter = timestampConverter;

    buildSystemExcludeSpanRuleConfigs(config);
  }

  private void buildSystemExcludeSpanRuleConfigs(Config config) {
    if (!config.hasPath(SYSTEM_EXCLUDE_SPAN_RULES)) {
      systemExcludeSpanRules = Collections.emptyList();
      systemExcludeSpanRuleIdToRuleMap = Collections.emptyMap();
      return;
    }
    List<? extends com.typesafe.config.ConfigObject> systemExcludeSpanRuleObjects =
        config.getObjectList(SYSTEM_EXCLUDE_SPAN_RULES);
    systemExcludeSpanRules = buildSystemExcludeSpanRules(systemExcludeSpanRuleObjects);
    systemExcludeSpanRuleIdToRuleMap = buildExcludeSpanRuleIdToRuleMap(systemExcludeSpanRules);
  }

  @Override
  public void getAllExcludeSpanRules(
      GetAllExcludeSpanRulesRequest request,
      StreamObserver<GetAllExcludeSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      List<ExcludeSpanRuleDetails> userExcludeSpanRules =
          excludeSpanRulesConfigStore.getAllData(requestContext);

      // reorder user exclude span rules and system exclude span rules as per expected priority
      List<ExcludeSpanRuleDetails> excludeSpanRules =
          reorderExcludeSpanRules(userExcludeSpanRules, systemExcludeSpanRuleIdToRuleMap);

      responseObserver.onNext(
          GetAllExcludeSpanRulesResponse.newBuilder().addAllRuleDetails(excludeSpanRules).build());
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

  private List<ExcludeSpanRuleDetails> reorderExcludeSpanRules(
      List<ExcludeSpanRuleDetails> userExcludeSpanRules,
      Map<String, ExcludeSpanRule> systemExcludeSpanRuleIdToRuleMap) {
    // This method sets the priority of rules. User exclude span rules -> user overridden system
    // exclude span rules -> non overridden system exclude span rules
    List<ExcludeSpanRuleDetails> excludeSpanRules = new ArrayList<>();
    List<ExcludeSpanRuleDetails> overriddenSystemExcludeSpanRules = new ArrayList<>();
    Set<String> overriddenSystemExcludeSpanRuleIds = new HashSet<>();

    for (ExcludeSpanRuleDetails excludeSpanRule : userExcludeSpanRules) {
      String id = excludeSpanRule.getRule().getId();
      if (systemExcludeSpanRuleIdToRuleMap.containsKey(id)) {
        // user overridden system exclude span rules
        overriddenSystemExcludeSpanRules.add(excludeSpanRule);
        overriddenSystemExcludeSpanRuleIds.add(id);
      } else {
        // user exclude span rules
        excludeSpanRules.add(excludeSpanRule);
      }
    }

    // non overridden system exclude span rules
    List<ExcludeSpanRuleDetails> nonOverriddenSystemExcludeSpanRules =
        systemExcludeSpanRules.stream()
            .filter(
                excludeSpanRule ->
                    !overriddenSystemExcludeSpanRuleIds.contains(excludeSpanRule.getId()))
            .map(
                excludeSpanRule ->
                    ExcludeSpanRuleDetails.newBuilder().setRule(excludeSpanRule).build())
            .collect(Collectors.toUnmodifiableList());

    excludeSpanRules.addAll(overriddenSystemExcludeSpanRules);
    excludeSpanRules.addAll(nonOverriddenSystemExcludeSpanRules);
    return excludeSpanRules;
  }

  private List<ExcludeSpanRule> buildSystemExcludeSpanRules(
      List<? extends com.typesafe.config.ConfigObject> configObjects) {
    return configObjects.stream()
        .map(this::buildExcludeSpanRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  private Map<String, ExcludeSpanRule> buildExcludeSpanRuleIdToRuleMap(
      List<ExcludeSpanRule> excludeSpanRules) {
    return excludeSpanRules.stream()
        .collect(Collectors.toUnmodifiableMap(ExcludeSpanRule::getId, Function.identity()));
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
}
