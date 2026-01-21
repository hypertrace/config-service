package ai.traceable.ipresolutionstrategy.config.service.v1;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceImplBase;
import ai.traceable.ipresolutionstrategy.config.service.v1.store.IpResolutionStrategyStore;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class IpResolutionStrategyConfigServiceImpl
    extends IpResolutionStrategyConfigServiceImplBase {

  private final IpResolutionStrategyStore store;
  private final UuidGenerator uuidGenerator;
  private final IpResolutionStrategyRequestValidator requestValidator;

  @Override
  public void getIpResolutionStrategyConfigs(
      GetIpResolutionStrategyConfigsRequest request,
      StreamObserver<GetIpResolutionStrategyConfigsResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(context, request);
      responseObserver.onNext(
          GetIpResolutionStrategyConfigsResponse.newBuilder()
              .addAllConfigs(store.getAllConfigData(context, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error fetching ip resolution strategies: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createIpResolutionStrategyConfig(
      CreateIpResolutionStrategyConfigRequest request,
      StreamObserver<CreateIpResolutionStrategyConfigResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(context, request);
      IpResolutionStrategyConfigData data = request.getData();
      String id = uuidGenerator.generateRandomId();
      IpResolutionStrategyConfig config =
          IpResolutionStrategyConfig.newBuilder().setId(id).setData(data).build();
      ContextualConfigObject<IpResolutionStrategyConfig> result =
          store.upsertObject(context, config);
      responseObserver.onNext(
          CreateIpResolutionStrategyConfigResponse.newBuilder()
              .setConfig(result.getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating ip resolution strategy: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateIpResolutionStrategyConfig(
      UpdateIpResolutionStrategyConfigRequest request,
      StreamObserver<UpdateIpResolutionStrategyConfigResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(context, request);
      String id = request.getId();
      Optional<IpResolutionStrategyConfig> existing = store.getData(context, id);
      if (existing.isEmpty()) {
        responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        return;
      }
      IpResolutionStrategyConfig config =
          IpResolutionStrategyConfig.newBuilder().setId(id).setData(request.getData()).build();
      ContextualConfigObject<IpResolutionStrategyConfig> result =
          store.upsertObject(context, config);
      responseObserver.onNext(
          UpdateIpResolutionStrategyConfigResponse.newBuilder()
              .setConfig(result.getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error updating ip resolution strategy: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteIpResolutionStrategyConfig(
      DeleteIpResolutionStrategyConfigRequest request,
      StreamObserver<DeleteIpResolutionStrategyConfigResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(context, request);
      Optional<DeletedContextualConfigObject<IpResolutionStrategyConfig>> deleted =
          store.deleteObject(context, request.getId());
      if (deleted.isEmpty()) {
        responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        return;
      }
      responseObserver.onNext(DeleteIpResolutionStrategyConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error deleting ip resolution strategy: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
