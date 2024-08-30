package org.hypertrace.label.application.rule.config.service;

import static java.util.function.Function.identity;

import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
  private static final String LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG =
      "label.application.rule.config.service";
  private static final String MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT =
      "max.dynamic.label.application.rules.per.tenant";
  private static final String SYSTEM_LABEL_APPLICATION_RULES = "system.label.application.rules";
  private static final int DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT = 100;

  private final IdentifiedObjectStore<LabelApplicationRule> labelApplicationRuleStore;
  private final LabelApplicationRuleValidator requestValidator;
  private final int maxDynamicLabelApplicationRulesAllowed;
  private final List<LabelApplicationRule> systemLabelApplicationRules;
  private final Map<String, LabelApplicationRule> systemLabelApplicationRulesMap;

  public LabelApplicationRuleConfigServiceImpl(
      Channel configChannel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    Config labelApplicationRuleConfig =
        config.hasPath(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG)
            ? config.getConfig(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG)
            : ConfigFactory.empty();
    this.maxDynamicLabelApplicationRulesAllowed =
        labelApplicationRuleConfig.hasPath(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT)
            ? labelApplicationRuleConfig.getInt(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT)
            : DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT;
    if (labelApplicationRuleConfig.hasPath(SYSTEM_LABEL_APPLICATION_RULES)) {
      final List<? extends com.typesafe.config.ConfigObject> systemLabelApplicationRulesObjectList =
          labelApplicationRuleConfig.getObjectList(SYSTEM_LABEL_APPLICATION_RULES);
      this.systemLabelApplicationRules =
          buildSystemLabelApplicationRuleList(systemLabelApplicationRulesObjectList);
      this.systemLabelApplicationRulesMap =
          this.systemLabelApplicationRules.stream()
              .collect(Collectors.toUnmodifiableMap(LabelApplicationRule::getId, identity()));
    } else {
      this.systemLabelApplicationRules = Collections.emptyList();
      this.systemLabelApplicationRulesMap = Collections.emptyMap();
    }
    final ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.labelApplicationRuleStore =
        new LabelApplicationRuleStore(configServiceBlockingStub, configChangeEventGenerator);
    this.requestValidator = new LabelApplicationRuleValidatorImpl();
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
      List<LabelApplicationRule> labelApplicationRules =
          this.labelApplicationRuleStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Set<String> labelApplicationRuleIds =
          labelApplicationRules.stream()
              .map(LabelApplicationRule::getId)
              .collect(Collectors.toUnmodifiableSet());
      List<LabelApplicationRule> filteredSystemLabelApplicationRules =
          systemLabelApplicationRules.stream()
              .filter(rule -> !labelApplicationRuleIds.contains(rule.getId()))
              .collect(Collectors.toUnmodifiableList());
      responseObserver.onNext(
          GetLabelApplicationRulesResponse.newBuilder()
              .addAllLabelApplicationRules(labelApplicationRules)
              .addAllLabelApplicationRules(filteredSystemLabelApplicationRules)
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
              .or(() -> Optional.ofNullable(systemLabelApplicationRulesMap.get(request.getId())))
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
      String labelApplicationRuleId = request.getId();
      if (systemLabelApplicationRulesMap.containsKey(labelApplicationRuleId)) {
        // Deleting a system label application rule is not allowed
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
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

  private List<LabelApplicationRule> buildSystemLabelApplicationRuleList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(LabelApplicationRuleConfigServiceImpl::buildLabelApplicationRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static LabelApplicationRule buildLabelApplicationRuleFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
