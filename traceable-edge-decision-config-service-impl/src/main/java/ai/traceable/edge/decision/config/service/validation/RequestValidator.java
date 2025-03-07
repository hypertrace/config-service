package ai.traceable.edge.decision.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.proto.utils.ProtoUtils;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.JexlScriptConfig;
import ai.traceable.edge.decision.config.service.error.JexlParserException;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RequestValidator {
  private static final JexlEngine JEXL_VALIDATOR = new JexlBuilder().silent(false).create();

  public static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public static <T extends Message> void validateRequest(RequestContext requestContext, T request) {
    validateRequestContext(requestContext);
    var jexlExpressionConfigs = ProtoUtils.find(request, JexlExpressionConfig.class);
    for (var jexlExpressionConfig : jexlExpressionConfigs) {
      // validate transformation
      validate(jexlExpressionConfig);
    }
    var jexScriptConfigs = ProtoUtils.find(request, JexlScriptConfig.class);
    for (var jexlScriptConfig : jexScriptConfigs) {
      // validate transformation
      validate(jexlScriptConfig);
    }
  }

  // perform basic syntax validation. it doesn't "compile" the expression
  static void validate(JexlExpressionConfig expression) {
    // validate jexl expression
    try {
      JEXL_VALIDATOR.createExpression(expression.getJexlExpression());
    } catch (Exception e) {
      throw new JexlParserException(
          "Invalid JEXL expression: " + expression.getJexlExpression(), e);
    }
  }

  static void validate(JexlScriptConfig script) {
    // validate jexl script
    try {
      JEXL_VALIDATOR.createScript(script.getJexlScript());
    } catch (Exception e) {
      throw new JexlParserException("Invalid JEXL script: " + script.getJexlScript(), e);
    }
  }
}
