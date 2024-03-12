package org.hypertrace.label.application.rule.config.service;

import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesResponse;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleResponse;

public class LabelApplicationRuleConfigServiceImpl
    extends LabelApplicationRuleConfigServiceGrpc.LabelApplicationRuleConfigServiceImplBase {
  static final String LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG =
      "label.application.rule.config.service";
  static final String MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT =
      "max.dynamic.label.application.rules.per.tenant";
  private static final String SYSTEM_LABEL_APPLICATION_RULES =
      "label.application.rule.config.service.system.label.application.rules";
  static final int DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT = 100;
  private final IdentifiedObjectStore<LabelApplicationRule> labelApplicationRuleStore;
  private final LabelApplicationRuleValidator requestValidator;
  private final int maxDynamicLabelApplicationRulesAllowed;
  private Map<String, LabelApplicationRule> systemLabelApplicationRuleIdToRuleMap;

  public LabelApplicationRuleConfigServiceImpl(
      Channel configChannel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    int maxDynamicRules = DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT;
    if (config.hasPath(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG)) {
      Config labelApplicationRuleConfig =
          config.getConfig(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG);
      if (labelApplicationRuleConfig.hasPath(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT)) {
        maxDynamicRules =
            labelApplicationRuleConfig.getInt(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT);
      }
    }
    this.maxDynamicLabelApplicationRulesAllowed = maxDynamicRules;

    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.labelApplicationRuleStore =
        new LabelApplicationRuleStore(configServiceBlockingStub, configChangeEventGenerator);
    this.requestValidator = new LabelApplicationRuleValidatorImpl();

    buildSystemLabelApplicationRuleConfigs(config);
  }

  private void buildSystemLabelApplicationRuleConfigs(Config config) {
    if (!config.hasPath(SYSTEM_LABEL_APPLICATION_RULES)) {
      systemLabelApplicationRuleIdToRuleMap = Collections.emptyMap();
      return;
    }

    List<? extends com.typesafe.config.ConfigObject> systemLabelApplicationRuleObjects =
        config.getObjectList(SYSTEM_LABEL_APPLICATION_RULES);
    systemLabelApplicationRuleIdToRuleMap =
        buildSystemLabelApplicationRuleIdRuleMap(systemLabelApplicationRuleObjects);
  }

  @Override
  public void createLabelApplicationRule(
      CreateLabelApplicationRuleRequest request,
      StreamObserver<CreateLabelApplicationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      checkRequestForDynamicLabelsLimit(request, requestContext);
      LabelApplicationRule labelApplicationRule =
          LabelApplicationRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setData(request.getData())
              .build();
      LabelApplicationRule createdLabelApplicationRule =
          this.labelApplicationRuleStore
              .upsertObject(requestContext, labelApplicationRule)
              .getData();
      responseObserver.onNext(
          CreateLabelApplicationRuleResponse.newBuilder()
              .setLabelApplicationRule(createdLabelApplicationRule)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getLabelApplicationRules(
      GetLabelApplicationRulesRequest request,
      StreamObserver<GetLabelApplicationRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      List<LabelApplicationRule> userLabelApplicationRules =
          this.labelApplicationRuleStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());

      // reorder user label application rules and system label application rules as per expected
      // priority
      List<LabelApplicationRule> labelApplicationRules =
          reorderLabelApplicationRules(userLabelApplicationRules);

      responseObserver.onNext(
          GetLabelApplicationRulesResponse.newBuilder()
              .addAllLabelApplicationRules(labelApplicationRules)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateLabelApplicationRule(
      UpdateLabelApplicationRuleRequest request,
      StreamObserver<UpdateLabelApplicationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      LabelApplicationRule existingRule =
          this.labelApplicationRuleStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      LabelApplicationRule updateLabelApplicationRule =
          existingRule.toBuilder().setData(request.getData()).build();
      LabelApplicationRule upsertedLabelApplicationRule =
          this.labelApplicationRuleStore
              .upsertObject(requestContext, updateLabelApplicationRule)
              .getData();
      responseObserver.onNext(
          UpdateLabelApplicationRuleResponse.newBuilder()
              .setLabelApplicationRule(upsertedLabelApplicationRule)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteLabelApplicationRule(
      DeleteLabelApplicationRuleRequest request,
      StreamObserver<DeleteLabelApplicationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);

      // do not allow deleting system label application rules
      if (systemLabelApplicationRuleIdToRuleMap.containsKey(request.getId())) {
        throw Status.INVALID_ARGUMENT.asRuntimeException();
      }

      this.labelApplicationRuleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteLabelApplicationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private void checkRequestForDynamicLabelsLimit(
      CreateLabelApplicationRuleRequest request, RequestContext requestContext) {
    if (request.getData().getLabelAction().hasDynamicLabelKey()
        || request.getData().getLabelAction().hasDynamicLabelExpression()) {
      int dynamicLabelApplicationRules =
          (int)
              this.labelApplicationRuleStore.getAllObjects(requestContext).stream()
                  .map(configObject -> configObject.getData().getData().getLabelAction())
                  .filter(
                      action -> action.hasDynamicLabelExpression() || action.hasDynamicLabelKey())
                  .count();
      if (dynamicLabelApplicationRules >= maxDynamicLabelApplicationRulesAllowed) {
        throw Status.RESOURCE_EXHAUSTED.asRuntimeException();
      }
    }
  }

  private Map<String, LabelApplicationRule> buildSystemLabelApplicationRuleIdRuleMap(
      List<? extends com.typesafe.config.ConfigObject> configObjects) {
    return configObjects.stream()
        .map(this::buildLabelApplicationRuleFromConfig)
        .collect(Collectors.toUnmodifiableMap(LabelApplicationRule::getId, Function.identity()));
  }

  @SneakyThrows
  private LabelApplicationRule buildLabelApplicationRuleFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  private List<LabelApplicationRule> reorderLabelApplicationRules(
      List<LabelApplicationRule> userLabelApplicationRules) {
    // This method sets the priority of rules. User label application rules -> user overridden
    // system
    // label application rules -> non overridden system label application rules
    List<LabelApplicationRule> labelApplicationRules = new ArrayList<>();
    List<LabelApplicationRule> overriddenSystemLabelApplicationRules = new ArrayList<>();
    Set<String> overriddenSystemLabelApplicationRuleIds = new HashSet<>();

    for (LabelApplicationRule userLabelApplicationRule : userLabelApplicationRules) {
      String id = userLabelApplicationRule.getId();
      if (systemLabelApplicationRuleIdToRuleMap.containsKey(id)) {
        // user overridden system label application rules
        overriddenSystemLabelApplicationRules.add(userLabelApplicationRule);
        overriddenSystemLabelApplicationRuleIds.add(id);
      } else {
        // user label application rules
        labelApplicationRules.add(userLabelApplicationRule);
      }
    }

    // non overridden system label application rules
    List<LabelApplicationRule> nonOverriddenSystemLabelApplicationRules =
        systemLabelApplicationRuleIdToRuleMap.entrySet().stream()
            .filter(
                labelApplicationRuleEntry ->
                    !overriddenSystemLabelApplicationRuleIds.contains(
                        labelApplicationRuleEntry.getKey()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toUnmodifiableList());

    labelApplicationRules.addAll(overriddenSystemLabelApplicationRules);
    labelApplicationRules.addAll(nonOverriddenSystemLabelApplicationRules);
    return Collections.unmodifiableList(labelApplicationRules);
  }
}
