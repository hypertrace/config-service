package ai.traceable.saved.filter.config.service.validation;

import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;

import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultFieldMetadataGetterImpl implements FieldMetadataGetter {
  private static final Map<String, AttributeMetadata> SCOPE_DEFAULT_METADATA =
      Map.of("SPAN", AttributeMetadata.newBuilder().setValueKind(TYPE_STRING).build());

  private final AttributeServiceCachedClient attributeServiceCachedClient;
  private final JoinFeasibilityChecker joinFeasibilityChecker;

  @Override
  public AttributeMetadata getOrThrow(Field field, ValidationContext validationContext) {
    String effectiveScope = validationContext.scope();

    if (field.hasScope()) {
      joinFeasibilityChecker.validate(validationContext.scope(), field.getScope());
      effectiveScope = field.getScope();
    }

    return fetchAttributeMetadata(field.getKey(), effectiveScope, validationContext);
  }

  private AttributeMetadata fetchAttributeMetadata(
      String fieldName, String scope, ValidationContext validationContext) {
    Optional<AttributeMetadata> attributeMetadata =
        attributeServiceCachedClient.get(validationContext.requestContext(), scope, fieldName);

    if (attributeMetadata.isEmpty()) {
      return Optional.ofNullable(SCOPE_DEFAULT_METADATA.get(scope))
          .orElseThrow(
              () ->
                  Status.INVALID_ARGUMENT
                      .withDescription(
                          String.format(
                              "Invalid attribute key (%s) with scope (%s)", fieldName, scope))
                      .asRuntimeException(validationContext.requestContext().buildTrailers()));
    }

    return attributeMetadata.get();
  }
}
