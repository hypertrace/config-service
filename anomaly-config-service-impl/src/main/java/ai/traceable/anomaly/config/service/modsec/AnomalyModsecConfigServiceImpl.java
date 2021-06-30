package ai.traceable.anomaly.config.service.modsec;

import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecValidator;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceImplBase;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyModsecConfigServiceImpl extends AnomalyModsecConfigServiceImplBase {
  private final ModsecValidator validator;
  private final ModsecManager manager;

  @Inject
  public AnomalyModsecConfigServiceImpl(ModsecValidator validator, ModsecManager manager) {
    this.validator = validator;
    this.manager = manager;
  }

  @Override
  public void getModsecCrsRules(
      GetModsecCrsRulesRequest request,
      StreamObserver<GetModsecCrsRulesResponse> responseObserver) {
    Status status = validator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Anomaly Modsec Config Service Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetModsecCrsRulesResponse response =
          GetModsecCrsRulesResponse.newBuilder()
              .addAllModsecCrsRules(
                  manager.getModsecCrsRules(
                      RequestContext.CURRENT.get(),
                      new HashSet<>(request.getModsecCrsRulesTypesList())))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
