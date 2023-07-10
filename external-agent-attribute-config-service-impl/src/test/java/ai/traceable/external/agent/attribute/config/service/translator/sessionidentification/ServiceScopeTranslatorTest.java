package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.TestUtils.reader;
import static com.google.inject.Stage.DEVELOPMENT;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import com.google.inject.Guice;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ServiceScopeTranslatorTest {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();
  private final ServiceScopeTranslator translator =
      Guice.createInjector(DEVELOPMENT, new SessionIdentificationTranslationModule())
          .getInstance(ServiceScopeTranslator.class);

  @Test
  void test() throws IOException {
    Predicate predicate = translator.addServiceScopeRegexes(List.of("service1", "service2"));
    Predicate.Builder expectedPredicate = Predicate.newBuilder();
    PARSER.merge(
        reader("session_identification/service_scope/partial_output.json"), expectedPredicate);

    Assertions.assertEquals(expectedPredicate.build(), predicate);
  }
}
