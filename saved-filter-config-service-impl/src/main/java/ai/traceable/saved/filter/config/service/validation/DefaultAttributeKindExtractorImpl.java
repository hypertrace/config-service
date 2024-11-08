package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.FIELD;
import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.TYPE_NOT_SET;
import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.VALUE;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.Expression.TypeCase;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public class DefaultAttributeKindExtractorImpl implements AttributeKindExtractor {
  private final FieldMetadataGetter fieldMetadataGetter;
  private final ValueAttributeKindGetter valueAttributeKindGetter;
  private final EnumSwitcher<TypeCase, Expression, ValidationContext, AttributeKind> enumSwitcher;

  @Inject
  public DefaultAttributeKindExtractorImpl(
      FieldMetadataGetter fieldMetadataGetter, ValueAttributeKindGetter valueAttributeKindGetter) {
    this.fieldMetadataGetter = fieldMetadataGetter;
    this.valueAttributeKindGetter = valueAttributeKindGetter;
    this.enumSwitcher = buildSwitcher();
  }

  @Override
  public AttributeKind extractAttributeKind(
      Expression expression, ValidationContext validationContext) {

    return enumSwitcher.apply(expression.getTypeCase(), expression, validationContext);
  }

  private EnumSwitcher<TypeCase, Expression, ValidationContext, AttributeKind> buildSwitcher() {

    return EnumSwitcher.<TypeCase, Expression, ValidationContext, AttributeKind>builder(
            TypeCase.class)
        .addCase(FIELD, this::getAttributeFromField)
        .addCase(VALUE, this::getAttributeFromValue)
        .addExclusion(TYPE_NOT_SET)
        .exceptionSupplier(
            (expression, context, caseEnum) ->
                Status.INVALID_ARGUMENT
                    .withDescription(String.format("Invalid expression type %s", caseEnum))
                    .asRuntimeException())
        .build();
  }

  private AttributeKind getAttributeFromField(
      final Expression expression, final ValidationContext validationContext) {
    return fieldMetadataGetter.getOrThrow(expression.getField(), validationContext).getValueKind();
  }

  private AttributeKind getAttributeFromValue(
      final Expression expression, final ValidationContext validationContext) {
    return valueAttributeKindGetter.getForLiteralValue(expression.getValue());
  }
}
