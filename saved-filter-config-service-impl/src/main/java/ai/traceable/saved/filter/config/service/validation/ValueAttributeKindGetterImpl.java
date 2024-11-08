package ai.traceable.saved.filter.config.service.validation;

import static com.google.protobuf.Value.KindCase.BOOL_VALUE;
import static com.google.protobuf.Value.KindCase.KIND_NOT_SET;
import static com.google.protobuf.Value.KindCase.LIST_VALUE;
import static com.google.protobuf.Value.KindCase.NULL_VALUE;
import static com.google.protobuf.Value.KindCase.NUMBER_VALUE;
import static com.google.protobuf.Value.KindCase.STRING_VALUE;
import static com.google.protobuf.Value.KindCase.STRUCT_VALUE;
import static java.util.Map.entry;

import com.google.common.collect.Maps;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import io.grpc.Status;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public class ValueAttributeKindGetterImpl implements ValueAttributeKindGetter {

  private static final Map<KindCase, AttributeKind> PRIMITIVE_TYPE_MAP =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(NUMBER_VALUE, AttributeKind.TYPE_DOUBLE),
              entry(STRING_VALUE, AttributeKind.TYPE_STRING),
              entry(BOOL_VALUE, AttributeKind.TYPE_BOOL)));

  private static final Map<KindCase, AttributeKind> LIST_TYPE_MAP =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(NUMBER_VALUE, AttributeKind.TYPE_DOUBLE_ARRAY),
              entry(STRING_VALUE, AttributeKind.TYPE_STRING_ARRAY),
              entry(BOOL_VALUE, AttributeKind.TYPE_BOOL_ARRAY)));

  private final EnumSwitcher<KindCase, Value, Void, AttributeKind> enumSwitcher;

  public ValueAttributeKindGetterImpl() {
    this.enumSwitcher = buildSwitcher();
  }

  @Override
  public AttributeKind getForLiteralValue(Value value) {
    return enumSwitcher.apply(value.getKindCase(), value, null);
  }

  private EnumSwitcher<KindCase, Value, Void, AttributeKind> buildSwitcher() {

    return EnumSwitcher.<KindCase, Value, Void, AttributeKind>builder(KindCase.class)
        .addCase(LIST_VALUE, this::getFromListValue)
        .addCase(BOOL_VALUE, this::getFromPrimitiveValue)
        .addCase(STRING_VALUE, this::getFromPrimitiveValue)
        .addCase(NUMBER_VALUE, this::getFromPrimitiveValue)
        .addExclusion(KIND_NOT_SET)
        .addExclusion(STRUCT_VALUE)
        .addExclusion(NULL_VALUE)
        .exceptionSupplier(
            (expression, context, caseEnum) ->
                Status.INVALID_ARGUMENT
                    .withDescription("Invalid literal type: " + caseEnum)
                    .asRuntimeException())
        .build();
  }

  private AttributeKind getFromListValue(final Value value, final Void empty) {

    return buildListDatatype(value);
  }

  private AttributeKind getFromPrimitiveValue(final Value value, final Void empty) {

    return Optional.ofNullable(PRIMITIVE_TYPE_MAP.get(value.getKindCase()))
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription(String.format("%s is not a primitive", value))
                    .asRuntimeException());
  }

  private AttributeKind buildListDatatype(final Value literalValue) {
    if (literalValue.getListValue().getValuesList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Null value is not supported ")
          .asRuntimeException();
    }

    return extractDatatypeFromNonEmptyListValue(literalValue);
  }

  private AttributeKind extractDatatypeFromNonEmptyListValue(final Value literalValue) {
    // Assuming all the array elements of the literal have the same type, we use the type of the
    // first element
    return Optional.ofNullable(
            LIST_TYPE_MAP.get(literalValue.getListValue().getValuesList().get(0).getKindCase()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("%s is not a valid type", literalValue)));
  }
}
