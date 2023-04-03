package ai.traceable.ast.hooks.config.service;

import ai.traceable.ast.hooks.config.service.handlers.CreateAstHookHandler;
import ai.traceable.ast.hooks.config.service.handlers.UpdateAstHookHandler;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHooksConfigServiceGrpc.AstHooksConfigServiceImplBase;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAllAstHooksRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAllAstHooksResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AstHooksConfigServiceImpl extends AstHooksConfigServiceImplBase {
  private final RequestValidator requestValidator;
  private final CreateAstHookHandler createAstHookHandler;
  private final UpdateAstHookHandler updateAstHookHandler;
  private final AstHooksConfigStore configStore;

  @Override
  public void createAstHook(
      CreateAstHookRequest request, StreamObserver<CreateAstHookResponse> responseObserver) {
    try {
      requestValidator.validate(request);
      AstHook upsertedHook = createAstHookHandler.createHook(request, RequestContext.CURRENT.get());
      CreateAstHookResponse response =
          CreateAstHookResponse.newBuilder().setAstHook(upsertedHook).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating hook for request " + request);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllAstHooks(
      GetAllAstHooksRequest request, StreamObserver<GetAllAstHooksResponse> responseObserver) {
    try {
      List<AstHook> astHooks = configStore.getAllConfigData(RequestContext.CURRENT.get());
      GetAllAstHooksResponse response =
          GetAllAstHooksResponse.newBuilder().addAllAstHooks(astHooks).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting all ast hooks");
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAstHook(
      GetAstHookRequest request, StreamObserver<GetAstHookResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      AstHook astHook =
          configStore
              .getData(requestContext, request.getId())
              .orElseThrow(
                  () -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
      GetAstHookResponse response = GetAstHookResponse.newBuilder().setAstHook(astHook).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting ast hooks for id: " + request.getId(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAstHook(
      DeleteAstHookRequest request, StreamObserver<DeleteAstHookResponse> responseObserver) {
    try {
      requestValidator.validate(request);
      configStore.deleteObject(RequestContext.CURRENT.get(), request.getId());
      responseObserver.onNext(DeleteAstHookResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error deleting hooks for hook id " + request.getId());
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAstHook(
      UpdateAstHookRequest request, StreamObserver<UpdateAstHookResponse> responseObserver) {
    try {
      requestValidator.validate(request);
      AstHook astHook = updateAstHookHandler.updateHook(request, RequestContext.CURRENT.get());
      responseObserver.onNext(UpdateAstHookResponse.newBuilder().setAstHook(astHook).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error updating hook for id: " + request.getId(), e);
      responseObserver.onError(e);
    }
  }
}
