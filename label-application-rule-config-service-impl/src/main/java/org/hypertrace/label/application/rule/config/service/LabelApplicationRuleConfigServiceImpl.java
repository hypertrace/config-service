package org.hypertrace.label.application.rule.config.service;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.impl.v1.DeletedSystemLabelApplicationRule;
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
  private final IdentifiedObjectStore<LabelApplicationRule> labelApplicationRuleStore;
  private final IdentifiedObjectStore<DeletedSystemLabelApplicationRule>
      deletedSystemLabelApplicationRuleStore;
  private final LabelApplicationRuleValidator requestValidator;
  private final LabelApplicationRuleConfig labelApplicationRuleConfig;

  public LabelApplicationRuleConfigServiceImpl(
      Channel configChannel,
      LabelApplicationRuleConfig labelApplicationRuleConfig,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.labelApplicationRuleConfig = labelApplicationRuleConfig;

    final ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.labelApplicationRuleStore =
        new LabelApplicationRuleStore(configServiceBlockingStub, configChangeEventGenerator);
    this.deletedSystemLabelApplicationRuleStore =
        new DeletedSystemLabelApplicationRuleStore(
            configServiceBlockingStub, configChangeEventGenerator);
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
      Set<String> deletedSystemLabelApplicationRuleIds =
          getDeletedSystemLabelApplicationRuleIds(requestContext);
      List<LabelApplicationRule> filteredSystemLabelApplicationRules =
          this.labelApplicationRuleConfig.getSystemLabelApplicationRules().stream()
              .filter(rule -> !labelApplicationRuleIds.contains(rule.getId()))
              .filter(rule -> !deletedSystemLabelApplicationRuleIds.contains(rule.getId()))
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
              .or(() -> getSystemLabelApplicationRule(requestContext, request.getId()))
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
      boolean customRuleDeleted =
          this.labelApplicationRuleStore.deleteObject(requestContext, request.getId()).isPresent();
      Optional<LabelApplicationRule> systemLabelApplicationRule =
          getSystemLabelApplicationRule(requestContext, labelApplicationRuleId);
      if (systemLabelApplicationRule.isPresent()) {
        deletedSystemLabelApplicationRuleStore.upsertObject(
            requestContext,
            DeletedSystemLabelApplicationRule.newBuilder().setId(labelApplicationRuleId).build());
      }
      if (customRuleDeleted || systemLabelApplicationRule.isPresent()) {
        responseObserver.onNext(DeleteLabelApplicationRuleResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
      throw Status.NOT_FOUND.asRuntimeException();
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
      if (dynamicLabelApplicationRules
          >= this.labelApplicationRuleConfig.getMaxDynamicLabelApplicationRulesAllowed()) {
        throw Status.RESOURCE_EXHAUSTED.asRuntimeException();
      }
    }
  }

  private Set<String> getDeletedSystemLabelApplicationRuleIds(RequestContext requestContext) {
    return this.deletedSystemLabelApplicationRuleStore.getAllObjects(requestContext).stream()
        .map(ConfigObject::getData)
        .map(DeletedSystemLabelApplicationRule::getId)
        .collect(Collectors.toUnmodifiableSet());
  }

  private Optional<LabelApplicationRule> getSystemLabelApplicationRule(
      RequestContext requestContext, String id) {
    return this.labelApplicationRuleConfig
        .getSystemLabelApplicationRule(id)
        .filter(
            unused ->
                this.deletedSystemLabelApplicationRuleStore.getData(requestContext, id).isEmpty());
  }
}
