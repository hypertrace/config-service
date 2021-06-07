package ai.traceable.iprange.config.service;

import ai.traceable.iprange.config.service.rules.RulesManager;
import ai.traceable.iprange.config.service.rules.RulesValidator;
import ai.traceable.iprange.config.service.v1.*;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceImplBase;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpRangeConfigServiceImpl extends IpRangeConfigServiceImplBase {
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;

  @Inject
  IpRangeConfigServiceImpl(RulesValidator rulesValidator, RulesManager rulesManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
  }

  @Override
  public void getIpRangeRules(
      GetIpRangeRulesRequest request, StreamObserver<GetIpRangeRulesResponse> responseObserver) {
    try {
      List<IpRangeRule> ipRangeRules =
          rulesManager.getIpRangeRules(RequestContext.CURRENT.get(), request.getFilter());

      responseObserver.onNext(
          GetIpRangeRulesResponse.newBuilder().addAllRules(ipRangeRules).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch ip range rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createIpRangeRule(
      CreateIpRangeRuleRequest request,
      StreamObserver<CreateIpRangeRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Create Ip Range Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      IpRangeRule ipRangeRule =
          rulesManager.createIpRangeRule(RequestContext.CURRENT.get(), request);

      if (ipRangeRule == null) {
        log.error("Unable to create Ip Range Rule request: {}, returns null", request);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "Unable to create Ip Range Rule %s", request.getRuleDetails().getName()))
                .asException());
        return;
      }

      responseObserver.onNext(CreateIpRangeRuleResponse.newBuilder().setRule(ipRangeRule).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to create Ip Range Rule {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateIpRangeRule(
      UpdateIpRangeRuleRequest request,
      StreamObserver<UpdateIpRangeRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Update Ip Range Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      IpRangeRule updatedIpRangeRule =
          rulesManager.updateIpRangeRule(RequestContext.CURRENT.get(), request);
      if (updatedIpRangeRule == null) {
        log.error("Unable to update ip range rule request:{} returns null", request);
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format("Unable to update IP Range rule with id = %s", request.getId()))
                .asException());
        return;
      }

      responseObserver.onNext(
          UpdateIpRangeRuleResponse.newBuilder().setRule(updatedIpRangeRule).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update Ip Range Rule {} :", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteIpRangeRule(
      DeleteIpRangeRuleRequest request,
      StreamObserver<DeleteIpRangeRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Delete Ip Range Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      rulesManager.deleteIpRangeRule(RequestContext.CURRENT.get(), request.getId());
      responseObserver.onNext(DeleteIpRangeRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete ip range rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }
}
