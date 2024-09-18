package ai.traceable.ast.config.service.manager;

import ai.traceable.ast.config.service.store.AstOverridesStore;
import ai.traceable.ast.config.service.v1.AstOverride;
import ai.traceable.ast.config.service.v1.AstOverrideInfo;
import ai.traceable.ast.config.service.v1.CreateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.DeleteAstOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstOverridesRequest;
import ai.traceable.ast.config.service.v1.UpdateAstOverrideRequest;
import ai.traceable.config.utils.TimestampConverter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AstOverridesManager {

  private final AstOverridesStore astOverridesStore;
  private final TimestampConverter timestampConverter;

  public List<AstOverride> getAstOverrides(
      final RequestContext requestContext, final GetAstOverridesRequest request) {
    return astOverridesStore.getAllObjects(requestContext, request.getFilter()).stream()
        .map(this::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  public AstOverride createAstOverride(
      final RequestContext requestContext, final CreateAstOverrideRequest request) {
    final AstOverride astOverride = buildAstOverride(requestContext, request.getAstOverrideInfo());
    return convert(astOverridesStore.upsertObject(requestContext, astOverride));
  }

  public AstOverride updateAstOverride(
      final RequestContext requestContext, final UpdateAstOverrideRequest request) {
    final AstOverride existingAstOverride =
        astOverridesStore.getData(requestContext, request.getId()).orElseThrow();
    final AstOverride astOverride =
        buildAstOverride(requestContext, existingAstOverride, request.getAstOverrideInfo());
    return convert(astOverridesStore.upsertObject(requestContext, astOverride));
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

  private AstOverride convert(
      final ContextualConfigObject<AstOverride> astOverrideContextualConfigObject) {
    return astOverrideContextualConfigObject.getData().toBuilder()
        .setLastUpdatedTimestamp(
            timestampConverter.convert(astOverrideContextualConfigObject.getLastUpdatedTimestamp()))
        .build();
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
