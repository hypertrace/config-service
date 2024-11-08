package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy.DENY;
import static com.google.protobuf.Value.KindCase.LIST_VALUE;
import static com.google.protobuf.Value.KindCase.NULL_VALUE;
import static com.google.protobuf.Value.KindCase.STRUCT_VALUE;
import static java.util.function.Predicate.not;

import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ValueValidatorImpl implements ValueValidator {
  private static final Set<Value.KindCase> DISALLOWED_NESTED_VALUE_TYPES =
      Set.of(STRUCT_VALUE, LIST_VALUE, NULL_VALUE);
  private static final Set<Value.KindCase> DISALLOWED_FIRST_LEVEL_VALUE_TYPES =
      Set.of(STRUCT_VALUE, NULL_VALUE);

  @Override
  public void validate(Value value, NullValueStrategy nullValueStrategy) {

    Value.KindCase valueKind = value.getKindCase();

    boolean valueTypeNotAllowed = DISALLOWED_FIRST_LEVEL_VALUE_TYPES.contains(valueKind);

    if (valueTypeNotAllowed) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Value cannot contains: " + DISALLOWED_FIRST_LEVEL_VALUE_TYPES)
          .asRuntimeException();
    }

    switch (valueKind) {
      case LIST_VALUE:
        validateListValue(value.getListValue());
        break;
      case NULL_VALUE:
        validateNullValue(nullValueStrategy);
        break;
      default:
        // No struct values, so it's valid
        break;
    }
  }

  private void validateNullValue(NullValueStrategy nullValueStrategy) {

    if (DENY.equals(nullValueStrategy)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("NULL value is not allowed")
          .asRuntimeException();
    }
  }

  private void validateListValue(ListValue listValue) {
    List<Value> values = listValue.getValuesList();

    Optional<Value.KindCase> optionalKind = values.stream().map(Value::getKindCase).findFirst();

    if (optionalKind.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("List values cannot be empty")
          .asRuntimeException();
    }

    Value.KindCase firstValueType = optionalKind.orElseThrow();

    boolean valueTypeNotAllowed = DISALLOWED_NESTED_VALUE_TYPES.contains(firstValueType);

    if (valueTypeNotAllowed) {
      throw Status.INVALID_ARGUMENT
          .withDescription("List values cannot contains: " + DISALLOWED_NESTED_VALUE_TYPES)
          .asRuntimeException();
    }

    boolean hasDifferentTypes =
        values.stream().map(Value::getKindCase).anyMatch(not(firstValueType::equals));

    if (hasDifferentTypes) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Values in the list cannot have different types.")
          .asRuntimeException();
    }
  }
}
