package ai.traceable.fraud.policy.config.service.converter;

import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.FunctionParameter;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts TransformationPipeline to chained JEXL expressions.
 *
 * <p>For each function invocation in the pipeline, looks up the function's jexl_template, replaces
 * ${input} with the current expression, and substitutes parameter placeholders with actual or
 * default values.
 */
@Slf4j
@Singleton
public class PipelineToJexlConverter {

  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([^}]+)}");
  private static final String INPUT_PLACEHOLDER = "input";

  private final Map<String, TransformationFunction> functionById;

  @Inject
  public PipelineToJexlConverter(TransformationFunctionProvider functionProvider) {
    this.functionById = buildFunctionMap(functionProvider);
  }

  private static Map<String, TransformationFunction> buildFunctionMap(
      TransformationFunctionProvider provider) {
    Map<String, TransformationFunction> result = new HashMap<>();
    for (TransformationFunctionsByKind byKind : provider.getAllFunctions()) {
      for (TransformationFunction func : byKind.getFunctionsList()) {
        result.put(func.getId(), func);
      }
    }
    return result;
  }

  /**
   * Applies a transformation pipeline to a base JEXL expression.
   *
   * @param baseExpression the starting JEXL expression
   * @param pipeline the transformation pipeline to apply (may be null or empty)
   * @param entityId entity identifier for logging
   * @return the transformed JEXL expression with all pipeline functions chained
   */
  public String apply(String baseExpression, TransformationPipeline pipeline, String entityId) {
    if (baseExpression == null || baseExpression.isEmpty()) {
      return baseExpression;
    }

    if (pipeline == null || pipeline.getTransformationPipelineList().isEmpty()) {
      return baseExpression;
    }

    // Coerce the extracted value to a string before applying transformations.
    // This is a product-level constraint: all pipeline inputs are assumed to be strings.
    String currentExpr = "('' + " + baseExpression + ")";
    List<TransformationFunctionInvocation> invocations = pipeline.getTransformationPipelineList();

    for (TransformationFunctionInvocation invocation : invocations) {
      currentExpr = applyInvocation(currentExpr, invocation, entityId);
    }

    return currentExpr;
  }

  private String applyInvocation(
      String currentExpr, TransformationFunctionInvocation invocation, String entityId) {
    String functionId = invocation.getFunctionId();

    TransformationFunction function = functionById.get(functionId);
    if (function == null) {
      log.warn(
          "entityId={}, functionId={}, transformation function not found, skipping",
          entityId,
          functionId);
      return currentExpr;
    }

    String template = function.getJexlTemplate();
    if (template == null || template.isEmpty()) {
      log.warn("entityId={}, functionId={}, no JEXL template, skipping", entityId, functionId);
      return currentExpr;
    }

    return substituteTemplate(
        template, currentExpr, invocation.getParameterValuesMap(), function, entityId);
  }

  private String substituteTemplate(
      String template,
      String inputExpr,
      Map<String, Value> parameterValues,
      TransformationFunction function,
      String entityId) {

    StringBuilder result = new StringBuilder();
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);

    while (matcher.find()) {
      String placeholder = matcher.group(1);
      String replacement;

      if (INPUT_PLACEHOLDER.equals(placeholder)) {
        replacement = inputExpr;
      } else {
        replacement = resolveParameterValue(placeholder, parameterValues, function, entityId);
      }

      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  private String resolveParameterValue(
      String paramName,
      Map<String, Value> parameterValues,
      TransformationFunction function,
      String entityId) {

    if (parameterValues.containsKey(paramName)) {
      return JexlExpressionUtils.valueToString(parameterValues.get(paramName));
    }

    Optional<FunctionParameter> paramDef =
        function.getParametersList().stream()
            .filter(p -> p.getName().equals(paramName))
            .findFirst();

    if (paramDef.isPresent() && paramDef.get().hasDefaultValue()) {
      return JexlExpressionUtils.valueToString(paramDef.get().getDefaultValue());
    }

    log.warn(
        "entityId={}, functionId={}, parameter {} not found in invocation or defaults",
        entityId,
        function.getId(),
        paramName);
    return "";
  }
}
