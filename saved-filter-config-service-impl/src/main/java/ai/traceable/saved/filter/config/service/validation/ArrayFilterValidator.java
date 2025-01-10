package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.VALUE;

import ai.traceable.saved.filter.config.service.v1.ArrayFilterCondition;
import ai.traceable.saved.filter.config.service.v1.ArrayOperator;
import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import com.google.protobuf.Value;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public class ArrayFilterValidator implements SavedFilterValidator<ArrayFilterCondition> {
  private static final Set<ArrayOperator> DENYLISTED_OPERATORS =
      Set.of(ArrayOperator.ARRAY_OPERATOR_UNSPECIFIED, ArrayOperator.UNRECOGNIZED);

  private final ArrayContentTypeFetcher arrayContentTypeFetcher;
  private final Set<RelationalFilterInspector> relationalFilterInspectors;
  private final ExpressionValidator expressionValidator;
  private final AttributeKindExtractor attributeKindExtractor;
  private final EnumSwitcher<
          ArrayFilterCondition.TypeCase, ArrayFilterCondition, ValidationContext, Void>
      enumSwitcher;

  @Inject
  public ArrayFilterValidator(
      final ArrayContentTypeFetcher arrayContentTypeFetcher,
      final Set<RelationalFilterInspector> relationalFilterInspectors,
      final ExpressionValidator expressionValidator,
      final AttributeKindExtractor attributeKindExtractor) {
    this.arrayContentTypeFetcher = arrayContentTypeFetcher;
    this.relationalFilterInspectors = relationalFilterInspectors;
    this.expressionValidator = expressionValidator;
    this.attributeKindExtractor = attributeKindExtractor;
    this.enumSwitcher = buildEnumSwitcher();
  }

  @Override
  public void validate(
      final ArrayFilterCondition arrayFilter, final ValidationContext validationContext) {
    if (DENYLISTED_OPERATORS.contains(arrayFilter.getOperator())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Array filter operator is mandatory")
          .asRuntimeException();
    }

    enumSwitcher.apply(arrayFilter.getTypeCase(), arrayFilter, validationContext);
  }

  private EnumSwitcher<ArrayFilterCondition.TypeCase, ArrayFilterCondition, ValidationContext, Void>
      buildEnumSwitcher() {
    return EnumSwitcher
        .<ArrayFilterCondition.TypeCase, ArrayFilterCondition, ValidationContext, Void>builder(
            ArrayFilterCondition.TypeCase.class)
        .addCase(ArrayFilterCondition.TypeCase.RELATIONAL_FILTER, this::validateRelationalFilter)
        .addExclusion(ArrayFilterCondition.TypeCase.TYPE_NOT_SET)
        .exceptionSupplier(
            (expression, context, caseEnum) ->
                Status.UNIMPLEMENTED
                    .withDescription("Array filter type " + caseEnum + " is not supported")
                    .asRuntimeException())
        .build();
  }

  private void validateRelationalFilter(
      final ArrayFilterCondition arrayFilter, final ValidationContext validationContext) {
    final RelationalFilterCondition relationalFilterCondition = arrayFilter.getRelationalFilter();

    if (!relationalFilterCondition.getFieldName().isEmpty()
        || !Value.getDefaultInstance().equals(relationalFilterCondition.getFieldValue())) {
      // No need to perform any validation for the old flow
      return;
    }

    validateLhsExpression(relationalFilterCondition, validationContext);
    validateRhsExpression(relationalFilterCondition, validationContext);

    if (areBothSidesConstant(relationalFilterCondition)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "LHS and RHS both cannot be a constant in filter: %s", relationalFilterCondition))
          .asRuntimeException();
    }

    final AttributeKind lhsAttributeKind =
        extractAttributeKind(relationalFilterCondition.getLhsExpression(), validationContext);

    final AttributeKind lhsContentKind = arrayContentTypeFetcher.getContentType(lhsAttributeKind);

    final AttributeKind rhsAttributeKind =
        extractAttributeKind(relationalFilterCondition.getRhsExpression(), validationContext);

    final RelationalFilterInspector.RelationalFilterInspectionContext inspectionContext =
        RelationalFilterInspector.RelationalFilterInspectionContext.builder()
            .lhsAttributeKind(lhsContentKind)
            .rhsAttributeKind(rhsAttributeKind)
            .operator(relationalFilterCondition.getOperator())
            .loggingContext(arrayFilter.toString())
            .build();

    relationalFilterInspectors.forEach(
        inspector -> inspector.inspect(inspectionContext, validationContext));
  }

  private void validateLhsExpression(
      final RelationalFilterCondition filter, final ValidationContext validationContext) {
    expressionValidator.validate(
        filter.getLhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }

  private void validateRhsExpression(
      final RelationalFilterCondition filter, final ValidationContext validationContext) {
    expressionValidator.validate(
        filter.getRhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }

  private boolean areBothSidesConstant(final RelationalFilterCondition filter) {
    return VALUE.equals(filter.getLhsExpression().getTypeCase())
        && VALUE.equals(filter.getRhsExpression().getTypeCase());
  }

  private AttributeKind extractAttributeKind(
      Expression expression, ValidationContext validationContext) {
    try {
      return attributeKindExtractor.extractAttributeKind(expression, validationContext);
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Unable to extract attributeKind from expression: %s", expression))
          .asRuntimeException();
    }
  }
}
