package ai.traceable.customsignature.config.service;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.rules.RulesManager;
import ai.traceable.customsignature.config.service.rules.RulesValidator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureConfigServiceImpl
    extends CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceImplBase {

  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final ModsecRulesManager modsecRulesManager;

  @Inject
  public CustomSignatureConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      ModsecRulesManager modsecRulesManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.modsecRulesManager = modsecRulesManager;
  }

  @Override
  public void getCustomSignatureRules(
      GetCustomSignatureRulesRequest request,
      StreamObserver<GetCustomSignatureRulesResponse> responseObserver) {
    try {
      List<CustomSignatureRule> rules =
          rulesManager.getCustomSignatureRules(RequestContext.CURRENT.get(), request.getFilter());
      responseObserver.onNext(
          GetCustomSignatureRulesResponse.newBuilder().addAllRules(rules).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("Unable to fetch custom signature rules"))
              .asException());
    }
  }

  @Override
  public void createCustomSignatureRule(
      CreateCustomSignatureRuleRequest request,
      StreamObserver<CreateCustomSignatureRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      log.error("Create Custom Signature Rule Request is not valid: ", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    Optional<CustomSignatureRule> customSignatureRuleOptional =
        rulesManager.createCustomSignatureRule(RequestContext.CURRENT.get(), request);
    if (customSignatureRuleOptional.isEmpty()) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Unable to create custom signature rule %s", request.getName()))
              .asException());
      return;
    }
    responseObserver.onNext(
        CreateCustomSignatureRuleResponse.newBuilder()
            .setRule(customSignatureRuleOptional.get())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateCustomSignatureRule(
      UpdateCustomSignatureRuleRequest request,
      StreamObserver<UpdateCustomSignatureRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      log.error("Update Custom Signature Rule Request is not valid: ", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    Optional<CustomSignatureRule> customSignatureRuleOptional =
        rulesManager.updateCustomSignatureRule(RequestContext.CURRENT.get(), request.getRule());
    if (customSignatureRuleOptional.isEmpty()) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format(
                      "Unable to update custom signature rule %s", request.getRule().getId()))
              .asException());
      return;
    }
    responseObserver.onNext(
        UpdateCustomSignatureRuleResponse.newBuilder()
            .setRule(customSignatureRuleOptional.get())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteCustomSignatureRule(
      DeleteCustomSignatureRuleRequest request,
      StreamObserver<DeleteCustomSignatureRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      log.error("Delete Custom Signature Rule Request is not valid: ", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    String ruleId = request.getId();
    boolean isDeleted =
        rulesManager.deleteCustomSignatureRule(RequestContext.CURRENT.get(), ruleId);
    if (isDeleted) {
      responseObserver.onNext(DeleteCustomSignatureRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("Unable to delete custom signature rule %s", ruleId))
              .asException());
    }
  }

  @Override
  public void getCustomSignatureModsecRules(
      GetCustomSignatureModsecRulesRequest request,
      StreamObserver<GetCustomSignatureModsecRulesResponse> responseObserver) {
    try {
      List<CustomSignatureRule> rules =
          rulesManager.getCustomSignatureRules(RequestContext.CURRENT.get(), request.getFilter());
      GetCustomSignatureModsecRulesResponse response = modsecRulesManager.getModsecRules(rules);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("Unable to fetch modsec custom signature rules"))
              .asException());
    }
  }
}
