package ai.traceable.edge.decision.config.service.validation;

import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.JexlScriptConfig;
import ai.traceable.edge.decision.config.service.error.JexlParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequestValidatorTest {
  @Test
  public void testValidateJexlExpression() {
    Assertions.assertDoesNotThrow(() -> validateJexlExpression("expression"));
    Assertions.assertThrows(JexlParserException.class, () -> validateJexlExpression("expression("));
    Assertions.assertDoesNotThrow(
        () ->
            validateJexlExpression(
                "parsed_request_gql.getNumBatches() > 10 || parsed_request_gql.getDepth() > 10 || parsed_request_gql.getNumDuplicates() > 100 || parsed_request_gql.getNumAliases() > 100 || parsed_request_gql.getQueryComplexity() > 100 || parsed_request_gql.isHasCircularReferences() || (parsed_request_gql.getSelectionSets().contains('systemHealth')) || (parsed_request_gql.getSelectionSets().contains('systemUpdate')) || (parsed_request_gql.getVisitedFields().contains('systemHealth')) || (parsed_request_gql.getVisitedFields().contains('systemUpdate'))"));
    Assertions.assertThrows(
        JexlParserException.class,
        () ->
            validateJexlExpression(
                "parsed_request_gql.getNumBatches() > 10 || parsed_request_gql.getDepth) > 10 || parsed_request_gql.getNumDuplicates() > 100 || parsed_request_gql.getNumAliases() > 100 || parsed_request_gql.getQueryComplexity() > 100 || parsed_request_gql.isHasCircularReferences() || (parsed_request_gql.getSelectionSets().contains('systemHealth')) || (parsed_request_gql.getSelectionSets().contains('systemUpdate')) || (parsed_request_gql.getVisitedFields().contains('systemHealth')) || (parsed_request_gql.getVisitedFields().contains('systemUpdate'))"));
    Assertions.assertThrows(JexlParserException.class, () -> validateJexlExpression("expression("));
  }

  @Test
  public void testValidateJexlScript() {
    Assertions.assertDoesNotThrow(() -> validateJexlScript("script"));
    Assertions.assertThrows(JexlParserException.class, () -> validateJexlScript("script("));
    Assertions.assertDoesNotThrow(
        () ->
            validateJexlScript(
                "parsed_request_gql.getNumBatches() > 10 || parsed_request_gql.getDepth() > 10 || parsed_request_gql.getNumDuplicates() > 100 || parsed_request_gql.getNumAliases() > 100 || parsed_request_gql.getQueryComplexity() > 100 || parsed_request_gql.isHasCircularReferences() || (parsed_request_gql.getSelectionSets().contains('systemHealth')) || (parsed_request_gql.getSelectionSets().contains('systemUpdate')) || (parsed_request_gql.getVisitedFields().contains('systemHealth')) || (parsed_request_gql.getVisitedFields().contains('systemUpdate'))"));
    Assertions.assertThrows(
        JexlParserException.class,
        () ->
            validateJexlScript(
                "parsed_request_gql.getNumBatches() > 10 || parsed_request_gql.getDepth) > 10 || parsed_request_gql.getNumDuplicates() > 100 || parsed_request_gql.getNumAliases() > 100 || parsed_request_gql.getQueryComplexity() > 100 || parsed_request_gql.isHasCircularReferences() || (parsed_request_gql.getSelectionSets().contains('systemHealth')) || (parsed_request_gql.getSelectionSets().contains('systemUpdate')) || (parsed_request_gql.getVisitedFields().contains('systemHealth')) || (parsed_request_gql.getVisitedFields().contains('systemUpdate'))"));
    Assertions.assertThrows(JexlParserException.class, () -> validateJexlScript("script("));
    Assertions.assertDoesNotThrow(
        () ->
            validateJexlScript(
                "var s = 'hello';"
                    + "var i = 0;"
                    + "while (i < 10) {"
                    + "  s = s + ' ' + i;"
                    + "  i++;"
                    + "}"
                    + "s;"));

    Assertions.assertThrows(
        JexlParserException.class,
        () ->
            validateJexlScript(
                "var s = 'hello';"
                    + "var i = 0;"
                    + "while (i  10) {"
                    + "  s = s + ' ' + i;"
                    + "  i++;"
                    + "}"
                    + "s;"));
  }

  private void validateJexlExpression(String expression) {
    RequestValidator.validate(
        JexlExpressionConfig.newBuilder().setJexlExpression(expression).build());
  }

  private void validateJexlScript(String script) {
    RequestValidator.validate(JexlScriptConfig.newBuilder().setJexlScript(script).build());
  }
}
