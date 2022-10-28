package ai.traceable.malicioussources.config.service;

import ai.traceable.malicioussources.config.service.rules.RulesManager;
import ai.traceable.malicioussources.config.service.rules.RulesValidator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceImplBase;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesConfigServiceImpl extends MaliciousSourcesConfigServiceImplBase {
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;

  @Inject
  MaliciousSourcesConfigServiceImpl(RulesValidator rulesValidator, RulesManager rulesManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
  }

  @Override
  public void getMaliciousSourcesRules(
      GetMaliciousSourcesRulesRequest request,
      StreamObserver<GetMaliciousSourcesRulesResponse> responseObserver) {
    try {
      List<MaliciousSourcesRule> maliciousSourcesRules =
          rulesManager.getMaliciousSourcesRules(RequestContext.CURRENT.get(), request.getFilter());

      responseObserver.onNext(
          GetMaliciousSourcesRulesResponse.newBuilder().addAllRules(maliciousSourcesRules).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch malicious sources rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createMaliciousSourcesRule(
      CreateMaliciousSourcesRuleRequest request,
      StreamObserver<CreateMaliciousSourcesRuleResponse> responseObserver) {
    try {
      Status status =
          rulesValidator.validate(
              request, getBlockAllExceptRulesSupplier(RequestContext.CURRENT.get()));
      if (!status.isOk()) {
        log.error("Create Malicious Sources Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      MaliciousSourcesRule maliciousSourcesRule =
          rulesManager.createMaliciousSourcesRule(RequestContext.CURRENT.get(), request);

      responseObserver.onNext(
          CreateMaliciousSourcesRuleResponse.newBuilder().setRule(maliciousSourcesRule).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to create Malicious Sources Rule {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateMaliciousSourcesRule(
      UpdateMaliciousSourcesRuleRequest request,
      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseObserver) {
    try {
      Status status =
          rulesValidator.validate(
              request, getBlockAllExceptRulesSupplier(RequestContext.CURRENT.get()));
      if (!status.isOk()) {
        log.error("Update Malicious Sources Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      MaliciousSourcesRule updatedMaliciousSourcesRule =
          rulesManager.updateMaliciousSourcesRule(RequestContext.CURRENT.get(), request);
      responseObserver.onNext(
          UpdateMaliciousSourcesRuleResponse.newBuilder()
              .setRule(updatedMaliciousSourcesRule)
              .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to update Malicious Sources Rule {} :", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteMaliciousSourcesRule(
      DeleteMaliciousSourcesRuleRequest request,
      StreamObserver<DeleteMaliciousSourcesRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Delete Malicious Source Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      rulesManager.deleteMaliciousSourcesRule(RequestContext.CURRENT.get(), request.getId());
      responseObserver.onNext(DeleteMaliciousSourcesRuleResponse.newBuilder().build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to delete malicious source rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }

  private Supplier<List<MaliciousSourcesRule>> getBlockAllExceptRulesSupplier(
      RequestContext requestContext) {
    GetRulesFilter actionFilter =
        GetRulesFilter.newBuilder()
            .addRuleActionTypes(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
            .build();
    return () -> rulesManager.getMaliciousSourcesRules(requestContext, actionFilter);
  }
}
