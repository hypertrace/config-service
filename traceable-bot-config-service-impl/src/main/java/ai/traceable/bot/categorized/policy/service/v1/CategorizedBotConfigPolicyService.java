package ai.traceable.bot.categorized.policy.service.v1;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceImplBase;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.bot.categorized.policy.service.v1.translator.CategorizedBotConfigPolicyToEdgeDecisionTranslator;
import ai.traceable.bot.categorized.policy.service.v1.validation.CategorizedBotConfigPolicyRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public final class CategorizedBotConfigPolicyService
    extends CategorizedBotConfigPolicyServiceImplBase {

  private final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager;
  private final CategorizedBotConfigPolicyToEdgeDecisionTranslator translator;

  private Exception decorateException(
      final RequestContext requestContext, final Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }

  @Override
  public void createCategorizedBotConfigPolicy(
      final CreateCategorizedBotConfigPolicyRequest request,
      final StreamObserver<CreateCategorizedBotConfigPolicyResponse> responseObserver) {
    handleConfigOperation(
        request, responseObserver, categorizedBotConfigPolicyStoreManager::create);
  }

  @Override
  public void updateCategorizedBotConfigPolicy(
      final UpdateCategorizedBotConfigPolicyRequest request,
      final StreamObserver<UpdateCategorizedBotConfigPolicyResponse> responseObserver) {
    handleConfigOperation(
        request, responseObserver, categorizedBotConfigPolicyStoreManager::update);
  }

  @Override
  public void getCategorizedBotConfigPolicies(
      final GetCategorizedBotConfigPoliciesRequest request,
      final StreamObserver<GetCategorizedBotConfigPoliciesResponse> responseObserver) {
    handleConfigOperation(request, responseObserver, categorizedBotConfigPolicyStoreManager::get);
  }

  @Override
  public void deleteCategorizedBotConfigPolicy(
      final DeleteCategorizedBotConfigPolicyRequest request,
      final StreamObserver<DeleteCategorizedBotConfigPolicyResponse> responseObserver) {
    handleConfigOperation(
        request, responseObserver, categorizedBotConfigPolicyStoreManager::delete);
  }

  @Override
  public void getCategorizedBotConfigPolicyEdgeDecisionRules(
      final GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest request,
      final StreamObserver<GetCategorizedBotConfigPolicyEdgeDecisionRulesResponse>
          responseObserver) {
    handleConfigOperation(request, responseObserver, translator::translate);
  }

  // Define a common method to handle requests
  private <R, S> void handleConfigOperation(
      final R request,
      final StreamObserver<S> responseStreamObserver,
      final BiFunction<RequestContext, R, S> configOperation) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    CategorizedBotConfigPolicyRequestValidator.validateRequestContext(requestContext);
    try {
      responseStreamObserver.onNext(configOperation.apply(requestContext, request));
      responseStreamObserver.onCompleted();
    } catch (final Exception exception) {
      final Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Error while processing request: {} with context {}. Error: {}",
          request,
          requestContext,
          decoratedException);
      responseStreamObserver.onError(decoratedException);
    }
  }
}
