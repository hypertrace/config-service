package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EQ;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NEQ;
import static java.util.Collections.unmodifiableSet;

import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SavedFilterValidator<S> {

  void validate(S source, ValidationContext validationContext);

  @Value
  @Builder(toBuilder = true)
  @Accessors(fluent = true, chain = true)
  @EqualsAndHashCode(doNotUseGetters = true)
  @ToString(doNotUseGetters = true)
  class ValidationContext {
    RequestContext requestContext;
    String scope;
    @Builder.Default Set<Field> filterVariables = new LinkedHashSet<>();

    public void addFilterVariable(final Field field) {
      filterVariables.add(field);
    }

    public Set<Field> getFilterVariable() {
      return unmodifiableSet(filterVariables);
    }
  }

  enum NullValueStrategy {
    ALLOW,
    DENY,
    ;

    private static final List<RelationalOperator> ALLOWED_NULL_VALUE_OPERATORS =
        List.of(RELATIONAL_OPERATOR_EQ, RELATIONAL_OPERATOR_NEQ);

    public static NullValueStrategy forRelationalOperator(final RelationalOperator operator) {
      return ALLOWED_NULL_VALUE_OPERATORS.contains(operator) ? ALLOW : DENY;
    }
  }
}
