package ai.traceable.genai.config.service.v1;

import ai.traceable.genai.config.service.v1.manager.GenAiConfigManager;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiConfigServiceImpl extends GenAiConfigServiceGrpc.GenAiConfigServiceImplBase {

  private final GenAiConfigManager genAiConfigManager;

  @Override
  public void getGenAiConfig(
      GetGenAiConfigRequest request, StreamObserver<GetGenAiConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      GenAiConfig genAiConfig =
          genAiConfigManager.getGenAiConfig(requestContext, request.getScope());
      responseObserver.onNext(
          GetGenAiConfigResponse.newBuilder().setGenAiConfig(genAiConfig).build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(
          "Get genAi config failed with the request: {} and context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateGenAiConfig(
      UpdateGenAiConfigRequest request,
      StreamObserver<UpdateGenAiConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      GenAiConfig genAiConfig =
          genAiConfigManager.updateGenAiConfig(
              requestContext, request.getScope(), request.getUpdate());
      responseObserver.onNext(
          UpdateGenAiConfigResponse.newBuilder().setGenAiConfig(genAiConfig).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update genAi config failed with the request: {} and context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
