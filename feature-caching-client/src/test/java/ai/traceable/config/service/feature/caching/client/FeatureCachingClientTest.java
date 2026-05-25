package ai.traceable.config.service.feature.caching.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import ai.traceable.featureflag.v1.FeatureFlagValue;
import com.google.common.cache.LoadingCache;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeatureCachingClientTest {
  private static final String GENAI_SERVER_SPAN_AI_CLASSIFICATION_ENABLED =
      "genai.server-span-ai-classification";

  @SuppressWarnings("unchecked")
  private final LoadingCache<ContextualKey<Void>, Map<String, FeatureFlagValue>> mockCache =
      mock(LoadingCache.class);

  @SuppressWarnings("unchecked")
  private final ContextualKey<Void> contextualKey = mock(ContextualKey.class);

  private FeatureCachingClient client;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() throws Exception {
    client = mock(FeatureCachingClient.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    Field cacheField = FeatureCachingClient.class.getDeclaredField("featureFlagCache");
    cacheField.setAccessible(true);
    cacheField.set(client, mockCache);
    requestContext = mock(RequestContext.class);
    when(requestContext.buildInternalContextualKey()).thenReturn(contextualKey);
  }

  @Test
  void isGenAiServerSpanAiClassificationEnabled_flagTrue_returnsTrue() throws Exception {
    Map<String, FeatureFlagValue> flagValues = new HashMap<>();
    flagValues.put(
        GENAI_SERVER_SPAN_AI_CLASSIFICATION_ENABLED,
        FeatureFlagValue.newBuilder().setBoolean(true).build());
    when(mockCache.get(contextualKey)).thenReturn(flagValues);

    assertTrue(client.isGenAiServerSpanAiClassificationEnabled(requestContext));
  }

  @Test
  void isGenAiServerSpanAiClassificationEnabled_flagFalse_returnsFalse() throws Exception {
    Map<String, FeatureFlagValue> flagValues = new HashMap<>();
    flagValues.put(
        GENAI_SERVER_SPAN_AI_CLASSIFICATION_ENABLED,
        FeatureFlagValue.newBuilder().setBoolean(false).build());
    when(mockCache.get(contextualKey)).thenReturn(flagValues);

    assertFalse(client.isGenAiServerSpanAiClassificationEnabled(requestContext));
  }

  @Test
  void isGenAiServerSpanAiClassificationEnabled_flagMissing_returnsDefault() throws Exception {
    when(mockCache.get(contextualKey)).thenReturn(new HashMap<>());

    assertFalse(client.isGenAiServerSpanAiClassificationEnabled(requestContext));
  }

  @Test
  void isGenAiServerSpanAiClassificationEnabled_cacheThrows_returnsDefault() throws Exception {
    when(mockCache.get(any())).thenThrow(new RuntimeException("cache failure"));

    assertFalse(client.isGenAiServerSpanAiClassificationEnabled(requestContext));
  }
}
