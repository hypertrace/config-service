package ai.traceable.ast.hooks.config.service;

import ai.traceable.ast.hooks.config.service.handlers.AstHookTestManager;
import ai.traceable.ast.hooks.config.service.handlers.CreateAstHookHandler;
import ai.traceable.ast.hooks.config.service.handlers.UpdateAstHookHandler;
import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.store.AstHooksTestConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestResult;
import ai.traceable.ast.hooks.config.service.v1.AstHooksConfigServiceGrpc.AstHooksConfigServiceImplBase;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestResponse;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookTestsResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAllAstHooksRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAllAstHooksResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestsResponse;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookResponse;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestResponse;
import ai.traceable.ast.hooks.config.service.validators.RequestValidator;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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
  private final AstHooksConfigStore astHooksConfigStore;
  private final AstHooksTestConfigStore astHooksTestConfigStore;
  private final AstHookTestManager astHookTestManager;

  @Override
  public void createAstHook(
      CreateAstHookRequest request, StreamObserver<CreateAstHookResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validate(requestContext, request);
      AstHook upsertedHook = createAstHookHandler.createHook(request, requestContext);
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
      List<AstHook> astHooks = astHooksConfigStore.getAllConfigData(RequestContext.CURRENT.get());
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
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      GetAstHookResponse response;

      // backward compatibility
      if (!request.getId().isBlank()) {
        AstHook astHook = astHooksConfigStore.getAstHook(requestContext, request.getId());
        response = GetAstHookResponse.newBuilder().setAstHook(astHook).build();
      } else {
        response =
            GetAstHookResponse.newBuilder()
                .addAllAstHooks(
                    astHooksConfigStore.getAllConfigData(requestContext, request.getFilter()))
                .build();
      }

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Error getting ast hooks for request: {} with context: {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteAstHook(
      DeleteAstHookRequest request, StreamObserver<DeleteAstHookResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validate(requestContext, request);
      List<String> idsToDelete = new ArrayList<>();
      if (!request.getId().isEmpty()) {
        idsToDelete.add(request.getId());
      } else if (!request.getIdsList().isEmpty()) {
        idsToDelete.addAll(request.getIdsList());
      }
      astHooksConfigStore.deleteObjects(requestContext, idsToDelete);
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
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validate(requestContext, request);
      AstHook astHook = updateAstHookHandler.updateHook(request, requestContext);
      responseObserver.onNext(UpdateAstHookResponse.newBuilder().setAstHook(astHook).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error updating hook for id: " + request.getId(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createAstHookTest(
      CreateAstHookTestRequest request,
      StreamObserver<CreateAstHookTestResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      HookConfig oldHookConfig = null;
      if (request.hasAstHookId()) {
        AstHook astHook = astHooksConfigStore.getAstHook(requestContext, request.getAstHookId());
        oldHookConfig = astHook.getHookDetails().getHookConfig();
      }
      responseObserver.onNext(
          CreateAstHookTestResponse.newBuilder()
              .setAstHookTest(
                  astHookTestManager.createHookTest(requestContext, request, oldHookConfig))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to create ast hook test config in context {} with request {}",
          requestContext,
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAstHookTest(
      UpdateAstHookTestRequest request,
      StreamObserver<UpdateAstHookTestResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateAstHookTestResponse.newBuilder()
              .setAstHookTest(astHookTestManager.updateHookTest(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to update ast hook test config in context {} with request {}",
          requestContext,
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAstHookTests(
      DeleteAstHookTestsRequest request,
      StreamObserver<DeleteAstHookTestsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      astHooksTestConfigStore.deleteObjects(requestContext, request.getIdsList());
      responseObserver.onNext(DeleteAstHookTestsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to delete ast hook test configs in context {} with request {}",
          requestContext,
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAstHookTestResult(
      GetAstHookTestResultRequest request,
      StreamObserver<GetAstHookTestResultResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      final AstHookTest astHookTest =
          astHooksTestConfigStore.getData(requestContext, request.getId()).orElseThrow();
      final AstHookTestResult astHookTestResult =
          AstHookTestResult.newBuilder()
              .setId(astHookTest.getId())
              .setTestStatus(astHookTest.getTestStatus())
              .addAllLogs(astHookTest.getLogsList())
              .build();
      responseObserver.onNext(
          GetAstHookTestResultResponse.newBuilder().setHookTestResult(astHookTestResult).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to fetch ast hook test result in context {} with request {}",
          requestContext,
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAstHookTests(
      GetAstHookTestsRequest request, StreamObserver<GetAstHookTestsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAstHookTestsResponse.newBuilder()
              .addAllAstHookTests(
                  astHooksTestConfigStore.getAllFilteredAstHookTests(
                      requestContext, request.getAstHookTestFiltersList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to fetch ast hook tests in context {} with request {}}",
          requestContext,
          request,
          e);
      responseObserver.onError(e);
    }
  }
}
