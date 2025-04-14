package org.hypertrace.config.service.store;

import com.google.protobuf.Value;
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
        // Infer and cast list element types (assumes homogeneous list)
        if (values.isEmpty()) {
          throw new IllegalArgumentException("Empty list not supported in ConstantExpression");
        }
        Value.KindCase elementType = values.get(0).getKindCase();
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
            throw new UnsupportedOperationException(
                "Unsupported list element type: " + elementType);
        }

      case STRUCT_VALUE:
        throw new UnsupportedOperationException("Struct not supported directly in this context");
      case NULL_VALUE:
      case KIND_NOT_SET:
      default:
        throw new IllegalArgumentException("Unsupported or null value in ConstantExpression");
    }
  }
}
