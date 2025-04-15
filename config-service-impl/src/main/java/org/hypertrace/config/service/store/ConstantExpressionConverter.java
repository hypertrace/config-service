package org.hypertrace.config.service.store;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;

public class ConstantExpressionConverter {

  public static ConstantExpression fromProtoValue(Value value) {
    switch (value.getKindCase()) {
      case STRING_VALUE:
        return ConstantExpression.of(value.getStringValue());
      case NUMBER_VALUE:
        return ConstantExpression.of(value.getNumberValue());
      case BOOL_VALUE:
        return ConstantExpression.of(value.getBoolValue());
      case LIST_VALUE:
        List<Value> values = value.getListValue().getValuesList();
        if (values.isEmpty()) {
          // Default to empty string list — or change logic based on expected behavior
          return ConstantExpression.ofStrings(List.of());
        }

        Value.KindCase elementType = values.get(0).getKindCase();
        boolean isHomogeneous = values.stream().allMatch(v -> v.getKindCase() == elementType);

        if (!isHomogeneous) {
          throw Status.INVALID_ARGUMENT
              .withDescription("List contains mixed types. All elements must be of the same type.")
              .asRuntimeException();
        }
        switch (elementType) {
          case STRING_VALUE:
            return ConstantExpression.ofStrings(
                values.stream().map(Value::getStringValue).collect(Collectors.toList()));
          case NUMBER_VALUE:
            return ConstantExpression.ofNumbers(
                values.stream().map(Value::getNumberValue).collect(Collectors.toList()));
          case BOOL_VALUE:
            return ConstantExpression.ofBooleans(
                values.stream().map(Value::getBoolValue).collect(Collectors.toList()));
          default:
            throw Status.UNIMPLEMENTED
                .withDescription("Unsupported list element type: " + elementType)
                .asRuntimeException();
        }
      case STRUCT_VALUE:
        throw Status.UNIMPLEMENTED
            .withDescription("Struct not supported directly in ConstantExpression")
            .asRuntimeException();
      case NULL_VALUE:
      case KIND_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported or null value in ConstantExpression")
            .asRuntimeException();
    }
  }
}
