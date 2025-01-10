package ai.traceable.saved.filter.config.service.validation;

import io.grpc.Status;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public class ArrayContentTypeFetcherImpl implements ArrayContentTypeFetcher {
  private static final Map<AttributeKind, AttributeKind> ARRAY_TYPE_TO_PRIMITIVE_MAP =
      Map.of(
          AttributeKind.TYPE_BOOL_ARRAY, AttributeKind.TYPE_BOOL,
          AttributeKind.TYPE_INT64_ARRAY, AttributeKind.TYPE_INT64,
          AttributeKind.TYPE_DOUBLE_ARRAY, AttributeKind.TYPE_DOUBLE,
          AttributeKind.TYPE_STRING_ARRAY, AttributeKind.TYPE_STRING);

  @Override
  public AttributeKind getContentType(final AttributeKind arrayType) {
    return Optional.ofNullable(ARRAY_TYPE_TO_PRIMITIVE_MAP.get(arrayType))
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription(String.format("Not a supported array type: %s", arrayType))
                    .asRuntimeException());
  }
}
