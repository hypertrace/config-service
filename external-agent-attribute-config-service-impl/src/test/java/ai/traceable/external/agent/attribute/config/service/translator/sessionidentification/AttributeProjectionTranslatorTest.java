package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AttributeProjectionTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final AttributeProjectionTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(AttributeProjectionTranslator.class);

  @Test
  void testForRequest() throws IOException {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/attribute_projection/partial_input_for_request.json");
    List<Projector> projectors =
        translator.translateForRequest(
            rule.getTokenRules(0), RuleCreationSource.RULE_CREATION_SOURCE_UNSPECIFIED);

    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/attribute_projection/partial_output_for_request.json"),
        eachMatchingProjector);
    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }

  @Test
  void testForResponse() throws IOException {
    SessionIdentificationRule rule =
        TestUtils.getSessionIdentificationRule(
            "session_identification/attribute_projection/partial_input_for_response.json");
    List<Projector> projectors = translator.translateForResponse(rule.getTokenRules(0));

    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/attribute_projection/partial_output_for_response.json"),
        eachMatchingProjector);
    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }
}
