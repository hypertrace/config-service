package ai.traceable.genai.system.discovery.config.service.v1.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.genai.system.discovery.config.service.v1.Condition;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.KeyValueCondition;
import ai.traceable.genai.system.discovery.config.service.v1.LeafCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchCondition;
import ai.traceable.genai.system.discovery.config.service.v1.Operator;
import ai.traceable.genai.system.discovery.config.service.v1.ReferenceCondition;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidatorImpl;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Guards against the vendor-collision class described in ASP-2492: two vendor rules whose model
 * matchers overlap so that a single model value satisfies both, letting evaluation order silently
 * decide the vendor (e.g. Cohere {@code embed} prefixing Google {@code embedding}).
 *
 * <p>Covers both default rule files in the served map: {@code
 * default-genai-system-discovery-rules.conf} (base) and {@code
 * default-genai-system-discovery-server-span-rules.conf} (server-span).
 */
class GenAiSystemDiscoveryRuleCollisionTest {

  private static final String MODEL_REFERENCE_PATH = "aiModelLocations";
  private static final String MODEL_ATTRIBUTE_KEY = "model";

  // OpenAI and Azure OpenAI deliberately share a model namespace (gpt, tts, ...); they are
  // disambiguated by host, not by model prefix, so they are not a collision with each other.
  private static final Set<String> OPENAI_NAMESPACE = Set.of("OpenAI", "AzureOpenAI");

  private List<GenAiSystemDiscoveryRule> rules;

  @BeforeEach
  void setUp() {
    final FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    final RequestContext requestContext = mock(RequestContext.class);
    when(featureCachingClient.isGenAiServerSpanAiClassificationEnabled(requestContext))
        .thenReturn(true);
    final GenAiSystemDiscoveryConfig config =
        new GenAiSystemDiscoveryConfig(
            new GenAiSystemDiscoveryRulesValidatorImpl(), featureCachingClient);
    rules = new ArrayList<>(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext).values());
  }

  /**
   * Server-span behavioural guard: each model value (resolved via {@code aiModelLocations}) must
   * classify to exactly one vendor, including the ASP-2492 regression cases.
   */
  @ParameterizedTest
  @CsvSource({
    "gpt-4o, OpenAI",
    "tts-1, OpenAI",
    "dall-e-3, OpenAI",
    "whisper-1, OpenAI",
    "text-embedding-3-small, OpenAI",
    "text-moderation-latest, OpenAI",
    "babbage-002, OpenAI",
    "davinci-002, OpenAI",
    "claude-3-5-sonnet-20241022, Anthropic",
    "command-r-plus-08-2024, Cohere",
    "command, Cohere",
    "embed-english-v3.0, Cohere",
    "embed-multilingual-v3.0, Cohere",
    "rerank-v3.5, Cohere",
    "c4ai-aya-expanse-8b, Cohere",
    "tiny-aya-global, Cohere",
    "cohere-transcribe-03-2026, Cohere",
    "chat-bison-001, Google",
    "text-bison-001, Google",
    "embedding-gecko, Google",
    "embedding-001, Google",
    "gemini-1.5-pro, Google",
    "aqa, Google",
    "deepseek-chat, DeepSeek",
    "deepseek-reasoner, DeepSeek",
    "llama-3.1-405b, Llama",
    "mistral-large-latest, Mistral AI",
    "magistral-medium-2506, Mistral AI",
    "codestral-2501, Mistral AI",
    "stablelm-2-12b, Stability AI",
    "grok-2-latest, Grok",
  })
  void modelValueClassifiesToExactlyOneVendor(
      final String modelValue, final String expectedVendor) {
    final List<String> matchedVendors =
        rules.stream()
            .filter(rule -> ruleMatchesModelReference(rule, modelValue))
            .map(this::vendorOf)
            .collect(Collectors.toList());
    assertEquals(
        List.of(expectedVendor),
        matchedVendors,
        () -> String.format("'%s' matched vendors %s", modelValue, matchedVendors));
  }

  /**
   * Structural guard for the prefix-overlap class across both rule files: among rules that classify
   * by model value alone (no host/regex gate to disambiguate them), no vendor's prefix/exact
   * matcher may be a substring-prefix of another vendor's. The OpenAI/Azure shared namespace is
   * treated as one bucket. CONTAINS matchers are excluded (covered behaviourally above).
   */
  @Test
  void noModelPrefixCollidesAcrossVendors() {
    final Map<String, List<String>> prefixesByNamespace = new LinkedHashMap<>();
    for (final GenAiSystemDiscoveryRule rule : rules) {
      final Condition condition = rule.getGenAiSystemDiscoveryRuleData().getCondition();
      if (isHostGated(condition)) {
        continue;
      }
      final List<String> prefixes =
          modelTargetMatchers(condition).stream()
              .filter(
                  matcher ->
                      matcher.getOperator() == Operator.OPERATOR_STARTS_WITH
                          || matcher.getOperator() == Operator.OPERATOR_EQUALS)
              .map(MatchCondition::getValue)
              .collect(Collectors.toList());
      if (!prefixes.isEmpty()) {
        prefixesByNamespace
            .computeIfAbsent(namespaceOf(rule), key -> new ArrayList<>())
            .addAll(prefixes);
      }
    }

    final List<NamespacedPrefix> prefixes = new ArrayList<>();
    prefixesByNamespace.forEach(
        (namespace, values) ->
            values.forEach(value -> prefixes.add(new NamespacedPrefix(namespace, value))));

    final List<String> collisions = new ArrayList<>();
    for (int i = 0; i < prefixes.size(); i++) {
      for (int j = i + 1; j < prefixes.size(); j++) {
        final NamespacedPrefix left = prefixes.get(i);
        final NamespacedPrefix right = prefixes.get(j);
        // Overlap within a namespace (e.g. OpenAI/Azure sharing "gpt") is intentional.
        if (!left.namespace.equals(right.namespace)
            && (left.value.startsWith(right.value) || right.value.startsWith(left.value))) {
          collisions.add(
              String.format(
                  "%s '%s' collides with %s '%s'",
                  left.namespace, left.value, right.namespace, right.value));
        }
      }
    }
    assertTrue(collisions.isEmpty(), () -> "Model prefix collisions across vendors: " + collisions);
  }

  /** A model prefix paired with the vendor namespace that owns it. */
  private static final class NamespacedPrefix {
    private final String namespace;
    private final String value;

    private NamespacedPrefix(final String namespace, final String value) {
      this.namespace = namespace;
      this.value = value;
    }
  }

  private boolean ruleMatchesModelReference(
      final GenAiSystemDiscoveryRule rule, final String modelValue) {
    final List<MatchCondition> matchers = new ArrayList<>();
    collectModelReferenceMatchers(rule.getGenAiSystemDiscoveryRuleData().getCondition(), matchers);
    return matchers.stream().anyMatch(matcher -> matches(matcher, modelValue));
  }

  private boolean matches(final MatchCondition matcher, final String modelValue) {
    final String value = matcher.getValue();
    switch (matcher.getOperator()) {
      case OPERATOR_EQUALS:
        return modelValue.equals(value);
      case OPERATOR_CONTAINS:
        return modelValue.contains(value);
      case OPERATOR_STARTS_WITH:
        return modelValue.startsWith(value);
      default:
        return false;
    }
  }

  private String vendorOf(final GenAiSystemDiscoveryRule rule) {
    return rule.getGenAiSystemDiscoveryRuleData()
        .getGenAiInfoExtractionAction()
        .getProviderAction()
        .getStaticNameAction()
        .getValue();
  }

  private String namespaceOf(final GenAiSystemDiscoveryRule rule) {
    final String vendor = vendorOf(rule);
    return OPENAI_NAMESPACE.contains(vendor) ? "OpenAI/Azure" : vendor;
  }

  /** True if the rule is disambiguated by host (urlCondition or a host-embedding regex). */
  private boolean isHostGated(final Condition condition) {
    if (condition.hasCompositeCondition()) {
      return condition.getCompositeCondition().getChildrenList().stream()
          .anyMatch(this::isHostGated);
    }
    if (condition.hasLeafCondition()) {
      final LeafCondition leaf = condition.getLeafCondition();
      if (leaf.hasUrlCondition()) {
        return true;
      }
      if (leaf.hasReferenceCondition()
          && leaf.getReferenceCondition().getValueMatch().getOperator()
              == Operator.OPERATOR_MATCHES_REGEX) {
        return true;
      }
    }
    return false;
  }

  /** Reference matchers on {@code aiModelLocations} only (server-span model resolution). */
  private void collectModelReferenceMatchers(
      final Condition condition, final List<MatchCondition> matchers) {
    if (condition.hasCompositeCondition()) {
      condition
          .getCompositeCondition()
          .getChildrenList()
          .forEach(child -> collectModelReferenceMatchers(child, matchers));
    } else if (condition.hasLeafCondition()
        && condition.getLeafCondition().hasReferenceCondition()) {
      final ReferenceCondition referenceCondition =
          condition.getLeafCondition().getReferenceCondition();
      if (MODEL_REFERENCE_PATH.equals(referenceCondition.getReference().getPath())) {
        matchers.add(referenceCondition.getValueMatch());
      }
    }
  }

  /** Every matcher that reads the model value: {@code aiModelLocations} or a {@code model} key. */
  private List<MatchCondition> modelTargetMatchers(final Condition condition) {
    final List<MatchCondition> matchers = new ArrayList<>();
    collectModelTargetMatchers(condition, matchers);
    return matchers;
  }

  private void collectModelTargetMatchers(
      final Condition condition, final List<MatchCondition> matchers) {
    if (condition.hasCompositeCondition()) {
      condition
          .getCompositeCondition()
          .getChildrenList()
          .forEach(child -> collectModelTargetMatchers(child, matchers));
      return;
    }
    if (!condition.hasLeafCondition()) {
      return;
    }
    final LeafCondition leaf = condition.getLeafCondition();
    if (leaf.hasReferenceCondition()) {
      final ReferenceCondition referenceCondition = leaf.getReferenceCondition();
      if (MODEL_REFERENCE_PATH.equals(referenceCondition.getReference().getPath())) {
        matchers.add(referenceCondition.getValueMatch());
      }
    } else if (leaf.hasRequestBodyCondition()) {
      addIfModelKey(leaf.getRequestBodyCondition(), matchers);
    } else if (leaf.hasResponseBodyCondition()) {
      addIfModelKey(leaf.getResponseBodyCondition(), matchers);
    } else if (leaf.hasAttributeCondition()) {
      addIfModelKey(leaf.getAttributeCondition(), matchers);
    }
  }

  private void addIfModelKey(
      final KeyValueCondition keyValueCondition, final List<MatchCondition> matchers) {
    if (MODEL_ATTRIBUTE_KEY.equals(keyValueCondition.getKeyMatchCondition().getValue())) {
      matchers.add(keyValueCondition.getValueMatchCondition());
    }
  }
}
