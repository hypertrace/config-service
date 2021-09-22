package org.hypertrace.label.application.rule.config.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
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
  private final ConfigServiceCoordinator configServiceCoordinator;
  private final LabelApplicationRuleValidator requestValidator;

  public LabelApplicationRuleConfigServiceImpl(Channel configChannel) {
    this.configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
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
          configServiceCoordinator.upsertLabelApplicationRule(requestContext, labelApplicationRule);
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
      LabelApplicationRule labelApplicationRule =
          configServiceCoordinator.getLabelApplicationRule(requestContext, ruleId);
      responseObserver.onNext(
          GetLabelApplicationRuleResponse.newBuilder()
              .setLabelApplicationRule(labelApplicationRule)
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
          configServiceCoordinator.getLabelApplicationRules(requestContext);
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
      configServiceCoordinator.getLabelApplicationRule(requestContext, ruleId);
      LabelApplicationRule updateLabelApplicationRule =
          configServiceCoordinator.upsertLabelApplicationRule(requestContext, labelApplicationRule);
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
      configServiceCoordinator.getLabelApplicationRule(requestContext, ruleId);
      configServiceCoordinator.deleteLabelApplicationRule(requestContext, ruleId);
      responseObserver.onNext(DeleteLabelApplicationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
