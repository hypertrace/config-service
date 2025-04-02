package ai.traceable.bot.categorized.policy.service.v1.store;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyDetails;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyFilter;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = {@Inject})
public class CategorizedBotConfigPolicyStoreManager {

  private final CategorizedBotConfigPolicyStore categorizedBotConfigPolicyStore;
  private final UuidGenerator uuidGenerator;
  private final DefaultPolicyConfig defaultPolicyConfig;

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

    checkValidRequest(requestContext, updateCategorizedBotConfigPolicyRequest);

    final ContextualConfigObject<CategorizedBotConfigPolicy>
        categorizedBotConfigPolicyContextualConfigObject =
            categorizedBotConfigPolicyStore.upsertObject(
                requestContext,
                updateCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicy());
    return UpdateCategorizedBotConfigPolicyResponse.newBuilder()
        .setCategorizedBotConfigPolicy(categorizedBotConfigPolicyContextualConfigObject.getData())
        .build();
  }

  private void checkValidRequest(
      final RequestContext requestContext,
      final UpdateCategorizedBotConfigPolicyRequest updateCategorizedBotConfigPolicyRequest) {
    final String policyId =
        updateCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicy().getId();
    if (!defaultPolicyConfig.isDefaultPolicy(policyId)) {
      categorizedBotConfigPolicyStore
          .getData(
              requestContext,
              updateCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicy().getId())
          .orElseThrow(
              () ->
                  Status.NOT_FOUND
                      .withDescription("Policy Not found")
                      .asRuntimeException(requestContext.buildTrailers()));
    } else {
      final CategorizedBotConfigPolicyDetails defaultPolicyDetails =
          defaultPolicyConfig
              .getDefaultPolicy(policyId)
              .orElseThrow(
                  () ->
                      Status.NOT_FOUND
                          .withDescription("Policy Not found")
                          .asRuntimeException(requestContext.buildTrailers()))
              .getCategorizedBotPolicyDetails();
      final CategorizedBotConfigPolicyDetails updatedPolicyDetails =
          updateCategorizedBotConfigPolicyRequest
              .getCategorizedBotConfigPolicy()
              .getCategorizedBotPolicyDetails();
      if (!defaultPolicyDetails.getName().equals(updatedPolicyDetails.getName())
          || !defaultPolicyDetails.getDescription().equals(updatedPolicyDetails.getDescription())
          || !new HashSet<>(defaultPolicyDetails.getBotScopesList())
              .equals(new HashSet<>(updatedPolicyDetails.getBotScopesList()))
          || (defaultPolicyDetails.getIsDefaultPolicy()
              != updatedPolicyDetails.getIsDefaultPolicy())) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Cannot change default fields in default policies")
            .asRuntimeException(requestContext.buildTrailers());
      }
    }
  }

  public GetCategorizedBotConfigPoliciesResponse get(
      final RequestContext requestContext,
      final GetCategorizedBotConfigPoliciesRequest getCategorizedBotConfigPoliciesRequest) {
    final List<CategorizedBotConfigPolicy> categorizedBotConfigPolicies = new ArrayList<>();
    if (getCategorizedBotConfigPoliciesRequest.hasCategorizedBotConfigPolicyFilter()
        && !(getCategorizedBotConfigPoliciesRequest
                .getCategorizedBotConfigPolicyFilter()
                .getCategorizedBotConfigPolicyIdsList()
                .isEmpty()
            && getCategorizedBotConfigPoliciesRequest
                .getCategorizedBotConfigPolicyFilter()
                .getEnvironmentIdsList()
                .isEmpty())) {
      categorizedBotConfigPolicies.addAll(
          categorizedBotConfigPolicyStore.getAllConfigData(
              requestContext,
              getCategorizedBotConfigPoliciesRequest.getCategorizedBotConfigPolicyFilter()));
    } else {
      categorizedBotConfigPolicies.addAll(
          categorizedBotConfigPolicyStore.getAllConfigData(requestContext));
    }
    categorizedBotConfigPolicies.addAll(
        getUneditedDefaultPolicies(
            categorizedBotConfigPolicies,
            getCategorizedBotConfigPoliciesRequest.getCategorizedBotConfigPolicyFilter()));
    return GetCategorizedBotConfigPoliciesResponse.newBuilder()
        .addAllCategorizedBotConfigPolicies(categorizedBotConfigPolicies)
        .build();
  }

  // check if stored policies contains any modified default policy,
  // if not add the default policy to the returned list
  private List<CategorizedBotConfigPolicy> getUneditedDefaultPolicies(
      final List<CategorizedBotConfigPolicy> categorizedBotConfigPolicies,
      final CategorizedBotConfigPolicyFilter categorizedBotConfigPolicyFilter) {
    final Set<String> policyIds =
        categorizedBotConfigPolicies.stream()
            .map(CategorizedBotConfigPolicy::getId)
            .collect(Collectors.toSet());
    return defaultPolicyConfig.getDefaultPolicies().stream()
        .filter(defaultPolicy -> !policyIds.contains(defaultPolicy.getId()))
        .filter(
            policy ->
                categorizedBotConfigPolicyStore.botPolicyFilterMatch(
                    policy, categorizedBotConfigPolicyFilter))
        .collect(Collectors.toUnmodifiableList());
  }

  public DeleteCategorizedBotConfigPolicyResponse delete(
      final RequestContext requestContext,
      final DeleteCategorizedBotConfigPolicyRequest deleteCategorizedBotConfigPolicyRequest) {
    // Default policies can only be modified, can't be deleted
    if (defaultPolicyConfig.isDefaultPolicy(
        deleteCategorizedBotConfigPolicyRequest.getCategorizedBotConfigPolicyId())) {
      throw Status.UNIMPLEMENTED
          .withDescription("Delete operation is not supported for default policies")
          .asRuntimeException();
    }
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
