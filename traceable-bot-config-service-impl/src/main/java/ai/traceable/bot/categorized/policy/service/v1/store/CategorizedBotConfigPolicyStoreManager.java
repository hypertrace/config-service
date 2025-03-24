package ai.traceable.bot.categorized.policy.service.v1.store;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CreateCategorizedBotConfigPolicyRequest;
import ai.traceable.bot.categorized.policy.service.v1.CreateCategorizedBotConfigPolicyResponse;
import ai.traceable.bot.categorized.policy.service.v1.DeleteCategorizedBotConfigPolicyRequest;
import ai.traceable.bot.categorized.policy.service.v1.DeleteCategorizedBotConfigPolicyResponse;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPoliciesRequest;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPoliciesResponse;
import ai.traceable.bot.categorized.policy.service.v1.UpdateCategorizedBotConfigPolicyRequest;
import ai.traceable.bot.categorized.policy.service.v1.UpdateCategorizedBotConfigPolicyResponse;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CategorizedBotConfigPolicyStoreManager {

  private final CategorizedBotConfigPolicyStore categorizedBotConfigPolicyStore;
  private final UuidGenerator uuidGenerator;

  public CreateCategorizedBotConfigPolicyResponse create(
      final RequestContext requestContext,
      final CreateCategorizedBotConfigPolicyRequest createCategorizedBotConfigPolicyRequest) {
    final ContextualConfigObject<CategorizedBotConfigPolicy>
        categorizedBotConfigPolicyContextualConfigObject =
            categorizedBotConfigPolicyStore.upsertObject(
                requestContext,
                CategorizedBotConfigPolicy.newBuilder()
                    .setCategorizedBotPolicyDetails(
                        createCategorizedBotConfigPolicyRequest
                            .getCategorizedBotConfigPolicyDetails())
                    .setId(uuidGenerator.generateRandomId())
                    .build());
    return CreateCategorizedBotConfigPolicyResponse.newBuilder()
        .setCategorizedBotConfigPolicy(categorizedBotConfigPolicyContextualConfigObject.getData())
        .build();
  }

  public UpdateCategorizedBotConfigPolicyResponse update(
      final RequestContext requestContext,
      final UpdateCategorizedBotConfigPolicyRequest updateCategorizedBotConfigPolicyRequest) {

    checkPolicyExists(requestContext, updateCategorizedBotConfigPolicyRequest);

    final ContextualConfigObject<CategorizedBotConfigPolicy>
        categorizedBotConfigPolicyContextualConfigObject =
            categorizedBotConfigPolicyStore.upsertObject(
                requestContext,
                updateCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicy());
    return UpdateCategorizedBotConfigPolicyResponse.newBuilder()
        .setCategorizedBotConfigPolicy(categorizedBotConfigPolicyContextualConfigObject.getData())
        .build();
  }

  private void checkPolicyExists(
      final RequestContext requestContext,
      final UpdateCategorizedBotConfigPolicyRequest updateCategorizedBotConfigPolicyRequest) {
    categorizedBotConfigPolicyStore
        .getData(
            requestContext,
            updateCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicy().getId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription("Policy Not found")
                    .asRuntimeException(requestContext.buildTrailers()));
  }

  public GetCategorizedBotConfigPoliciesResponse get(
      final RequestContext requestContext,
      final GetCategorizedBotConfigPoliciesRequest getCategorizedBotConfigPoliciesRequest) {
    List<CategorizedBotConfigPolicy> categorizedBotConfigPolicies;
    if (getCategorizedBotConfigPoliciesRequest.hasCategorizedBotConfigPolicyFilter()
        && !getCategorizedBotConfigPoliciesRequest
            .getCategorizedBotConfigPolicyFilter()
            .getCategorizedBotConfigPolicyIdsList()
            .isEmpty()) {
      categorizedBotConfigPolicies =
          categorizedBotConfigPolicyStore.getAllConfigData(
              requestContext,
              getCategorizedBotConfigPoliciesRequest.getCategorizedBotConfigPolicyFilter());
    } else {
      categorizedBotConfigPolicies =
          categorizedBotConfigPolicyStore.getAllConfigData(requestContext);
    }
    return GetCategorizedBotConfigPoliciesResponse.newBuilder()
        .addAllCategorizedBotConfigPolicies(categorizedBotConfigPolicies)
        .build();
  }

  public DeleteCategorizedBotConfigPolicyResponse delete(
      final RequestContext requestContext,
      final DeleteCategorizedBotConfigPolicyRequest deleteCategorizedBotConfigPolicyRequest) {
    final Optional<DeletedContextualConfigObject<CategorizedBotConfigPolicy>>
        optionalDeletedContextualConfigObject =
            categorizedBotConfigPolicyStore.deleteObject(
                requestContext,
                deleteCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicyId());
    return optionalDeletedContextualConfigObject
        .flatMap(DeletedConfigObject::getDeletedData)
        .map(
            categorizedBotConfigPolicy ->
                DeleteCategorizedBotConfigPolicyResponse.newBuilder()
                    .setCategorizedBotConfigPolicy(categorizedBotConfigPolicy)
                    .build())
        .orElseGet(DeleteCategorizedBotConfigPolicyResponse::getDefaultInstance);
  }
}
