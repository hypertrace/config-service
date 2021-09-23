package org.hypertrace.label.application.rule.config.service;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesResponse;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleResponse;

public class LabelApplicationRuleConfigServiceImpl
    extends LabelApplicationRuleConfigServiceGrpc.LabelApplicationRuleConfigServiceImplBase {
  public static final String LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME = "label-config";
  public static final String LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE =
      "label-application-rules";
  private final IdentifiedObjectStore<LabelApplicationRule> labelApplicationRuleStore;
  private final LabelApplicationRuleValidator requestValidator;

  public LabelApplicationRuleConfigServiceImpl(Channel configChannel) {
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.labelApplicationRuleStore =
        new LabelApplicationRuleStore(
            configServiceBlockingStub,
            LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE,
            LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME);
    this.requestValidator = new LabelApplicationRuleValidatorImpl();
  }

  @Override
  public void createLabelApplicationRule(
      CreateLabelApplicationRuleRequest request,
      StreamObserver<CreateLabelApplicationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      LabelApplicationRule labelApplicationRule =
          LabelApplicationRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setData(request.getData())
              .build();
      LabelApplicationRule createdLabelApplicationRule =
          this.labelApplicationRuleStore.upsertObject(requestContext, labelApplicationRule);
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
  public void getLabelApplicationRule(
      GetLabelApplicationRuleRequest request,
      StreamObserver<GetLabelApplicationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      String ruleId = request.getId();
      Optional<LabelApplicationRule> optionalLabelApplicationRule =
          this.labelApplicationRuleStore.getObject(requestContext, ruleId);
      if (optionalLabelApplicationRule.isEmpty()) {
        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
        return;
      }
      responseObserver.onNext(
          GetLabelApplicationRuleResponse.newBuilder()
              .setLabelApplicationRule(optionalLabelApplicationRule.get())
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
          this.labelApplicationRuleStore.getAllObjects(requestContext);
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
      String ruleId = request.getId();
      LabelApplicationRule labelApplicationRule =
          LabelApplicationRule.newBuilder().setId(ruleId).setData(request.getData()).build();
      if (this.labelApplicationRuleStore.getObject(requestContext, ruleId).isEmpty()) {
        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
        return;
      }
      LabelApplicationRule updateLabelApplicationRule =
          this.labelApplicationRuleStore.upsertObject(requestContext, labelApplicationRule);
      responseObserver.onNext(
          UpdateLabelApplicationRuleResponse.newBuilder()
              .setLabelApplicationRule(updateLabelApplicationRule)
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
      String ruleId = request.getId();
      if (this.labelApplicationRuleStore.getObject(requestContext, ruleId).isEmpty()) {
        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
        return;
      }
      ;
      this.labelApplicationRuleStore.deleteObject(requestContext, ruleId);
      responseObserver.onNext(DeleteLabelApplicationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
