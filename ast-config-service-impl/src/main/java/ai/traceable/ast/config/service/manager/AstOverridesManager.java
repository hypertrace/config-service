package ai.traceable.ast.config.service.manager;

import ai.traceable.ast.config.service.store.AstOverridesStore;
import ai.traceable.ast.config.service.v1.AstOverride;
import ai.traceable.ast.config.service.v1.AstOverrideInfo;
import ai.traceable.ast.config.service.v1.CreateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.DeleteAstOverridesRequest;
import ai.traceable.ast.config.service.v1.UpdateAstOverrideRequest;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AstOverridesManager {

  private final AstOverridesStore astOverridesStore;

  public List<AstOverride> getAstOverrides(final RequestContext requestContext) {
    return astOverridesStore.getAllConfigData(requestContext);
  }

  public AstOverride createAstOverride(
      final RequestContext requestContext, final CreateAstOverrideRequest request) {
    final AstOverride astOverride = buildAstOverride(requestContext, request.getAstOverrideInfo());
    return astOverridesStore.upsertObject(requestContext, astOverride).getData();
  }

  public AstOverride updateAstOverride(
      final RequestContext requestContext, final UpdateAstOverrideRequest request) {
    final AstOverride existingAstOverride =
        astOverridesStore.getData(requestContext, request.getId()).orElseThrow();
    final AstOverride astOverride =
        buildAstOverride(requestContext, existingAstOverride, request.getAstOverrideInfo());
    return astOverridesStore.upsertObject(requestContext, astOverride).getData();
  }

  public void deleteAstOverrides(
      final RequestContext requestContext, final DeleteAstOverridesRequest request) {
    astOverridesStore.deleteObjects(requestContext, request.getIdsList());
  }

  public boolean overrideExists(final RequestContext requestContext, final String id) {
    return astOverridesStore.getData(requestContext, id).isPresent();
  }

  private AstOverride buildAstOverride(
      final RequestContext requestContext,
      final AstOverride existingAstOverride,
      final AstOverrideInfo astOverrideInfo) {
    final AstOverride.Builder astOverrideBuilder =
        AstOverride.newBuilder()
            .setId(existingAstOverride.getId())
            .setName(astOverrideInfo.getName())
            .setIsEnabled(astOverrideInfo.getIsEnabled())
            .setCreatedBy(existingAstOverride.getCreatedBy())
            .addAllScopes(astOverrideInfo.getScopesList())
            .setConfig(astOverrideInfo.getConfig());
    if (astOverrideInfo.hasDescription()) {
      astOverrideBuilder.setDescription(astOverrideInfo.getDescription());
    }
    getRequestUser(requestContext).ifPresent(astOverrideBuilder::setLastUpdatedBy);
    return astOverrideBuilder.build();
  }

  private AstOverride buildAstOverride(
      final RequestContext requestContext, final AstOverrideInfo astOverrideInfo) {
    final AstOverride.Builder astOverrideBuilder =
        AstOverride.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setName(astOverrideInfo.getName())
            .setIsEnabled(astOverrideInfo.getIsEnabled())
            .addAllScopes(astOverrideInfo.getScopesList())
            .setConfig(astOverrideInfo.getConfig());
    if (astOverrideInfo.hasDescription()) {
      astOverrideBuilder.setDescription(astOverrideInfo.getDescription());
    }
    final Optional<String> requestUserMaybe = getRequestUser(requestContext);
    if (requestUserMaybe.isPresent()) {
      final String requestUser = requestUserMaybe.get();
      astOverrideBuilder.setCreatedBy(requestUser);
      astOverrideBuilder.setLastUpdatedBy(requestUser);
    }
    return astOverrideBuilder.build();
  }

  private Optional<String> getRequestUser(final RequestContext requestContext) {
    if (requestContext.getName().isPresent()) {
      return requestContext.getName();
    } else if (requestContext.getEmail().isPresent()) {
      return requestContext.getEmail();
    }
    return Optional.empty();
  }
}
