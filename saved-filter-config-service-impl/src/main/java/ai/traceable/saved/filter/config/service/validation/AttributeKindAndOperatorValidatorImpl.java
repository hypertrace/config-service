package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EQ;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_GREATER_THAN;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_GREATER_THAN_OR_EQUAL_TO;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_IN;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_LESS_THAN;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_LESS_THAN_OR_EQUAL_TO;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NEQ;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_IN;
import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_TIMESTAMP;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import com.google.common.collect.Maps;
import io.grpc.Status;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public class AttributeKindAndOperatorValidatorImpl implements AttributeKindAndOperatorValidator {

  public static final Map<AttributeKind, Set<RelationalOperator>>
      ATTRIBUTE_KIND_TO_SUPPORTED_OPERATORS_MAP =
          Maps.immutableEnumMap(
              Map.ofEntries(
                  entry(TYPE_BOOL, Set.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ)),
                  entry(
                      TYPE_INT64,
                      Set.of(
                          RELATIONAL_OPERATOR_EQ,
                          RELATIONAL_OPERATOR_NEQ,
                          RELATIONAL_OPERATOR_LESS_THAN,
                          RELATIONAL_OPERATOR_GREATER_THAN,
                          RELATIONAL_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_IN,
                          RELATIONAL_OPERATOR_NOT_IN)),
                  entry(
                      TYPE_DOUBLE,
                      Set.of(
                          RELATIONAL_OPERATOR_EQ,
                          RELATIONAL_OPERATOR_NEQ,
                          RELATIONAL_OPERATOR_LESS_THAN,
                          RELATIONAL_OPERATOR_GREATER_THAN,
                          RELATIONAL_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_IN,
                          RELATIONAL_OPERATOR_NOT_IN)),
                  entry(
                      TYPE_STRING,
                      Set.of(
                          RELATIONAL_OPERATOR_EQ,
                          RELATIONAL_OPERATOR_NEQ,
                          RELATIONAL_OPERATOR_LESS_THAN,
                          RELATIONAL_OPERATOR_GREATER_THAN,
                          RELATIONAL_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_IN,
                          RELATIONAL_OPERATOR_NOT_IN)),
                  entry(TYPE_STRING_ARRAY, Set.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ)),
                  entry(TYPE_INT64_ARRAY, Set.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ)),
                  entry(TYPE_DOUBLE_ARRAY, Set.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ)),
                  entry(TYPE_BOOL_ARRAY, Set.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ)),
                  entry(
                      TYPE_TIMESTAMP,
                      Set.of(
                          RELATIONAL_OPERATOR_EQ,
                          RELATIONAL_OPERATOR_NEQ,
                          RELATIONAL_OPERATOR_LESS_THAN,
                          RELATIONAL_OPERATOR_GREATER_THAN,
                          RELATIONAL_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                          RELATIONAL_OPERATOR_GREATER_THAN_OR_EQUAL_TO))));

  @Override
  public void validate(AttributeKind attributeKind, RelationalOperator operator) {

    Set<RelationalOperator> supportedOps =
        ATTRIBUTE_KIND_TO_SUPPORTED_OPERATORS_MAP.getOrDefault(attributeKind, emptySet());
    if (!supportedOps.contains(operator)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Unsupported operator %s for attributeKind %s", operator, attributeKind))
          .asRuntimeException();
    }
  }
}
