package ai.traceable.aiapp.protection.config.service.converter;

public final class AiAppConverterConstants {

  public static final String THREAT_TYPE_ID_LABEL_KEY = "threatTypeId";

  public static final String PII_DETECTED_IN_PROMPT_THREAT_TYPE_ID = "piiDetectedInPrompt";
  public static final String AI_RATE_LIMITING_THREAT_TYPE_ID = "llmRateLimiting";
  public static final String MODEL_GOVERNANCE_THREAT_TYPE_ID = "llmModelGovernance";
  public static final String AI_INPUT_EXPLOSION_THREAT_TYPE_ID = "llmInputExplosion";
  public static final String AI_SENSITIVE_DATA_PROTECTION_THREAT_TYPE_ID =
      "aiSensitiveDataProtection";

  public static final String GENAI_MODELS_ATTRIBUTE_KEY = "GENAI_MODELS";
  public static final String GENAI_PROVIDERS_ATTRIBUTE_KEY = "GENAI_PROVIDERS";
  public static final String GENAI_PROMPT_SIZE_ATTRIBUTE_KEY = "genAiPromptSize";
}
