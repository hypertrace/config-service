package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.sessionidentification.config.service.v1.Predicate;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AttributePredicateTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final AttributePredicateTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(AttributePredicateTranslator.class);

  @Test
  void test() throws IOException {
    Predicate.AttributePredicate.Builder builder = Predicate.AttributePredicate.newBuilder();
    PARSER.merge(reader("session_identification/attribute_predicate/partial_input.json"), builder);
    List<AttributeRule.Projector> projectors = translator.translate(builder.build());
    EachMatchingProjector.Builder eachMatchingProjector = EachMatchingProjector.newBuilder();
    PARSER.merge(
        reader("session_identification/attribute_predicate/partial_output.json"),
        eachMatchingProjector);

    Assertions.assertEquals(
        eachMatchingProjector.build().getAttributeRulesList().stream()
            .map(AttributeRule::getProjector)
            .collect(Collectors.toUnmodifiableList()),
        projectors);
  }
}
