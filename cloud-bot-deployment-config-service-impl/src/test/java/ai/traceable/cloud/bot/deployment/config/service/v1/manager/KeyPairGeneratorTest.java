package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails.JWTSigningAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyPairGeneratorTest {

  private KeyPairGenerator keyPairGenerator;

  @BeforeEach
  void setUp() {
    keyPairGenerator = new KeyPairGenerator();
  }

  @Test
  void generateJwtSigningKeyPair_shouldGenerateValidKeyPair() {
    JWTSigningKeyDetails keyDetails = keyPairGenerator.generateJwtSigningKeyPair();
    assertEquals(JWTSigningAlgorithm.JWT_SIGNING_ALGORITHM_RS256, keyDetails.getAlgorithm());
    assertTrue(keyDetails.getPublicKeyPem().startsWith("-----BEGIN PUBLIC KEY-----"));
    assertFalse(keyDetails.getEncryptedPrivateKey().getValue().startsWith("-----BE"));
  }

  @Test
  void generateJwtSigningKeyPair_shouldGenerateUniqueKeys() {
    JWTSigningKeyDetails keyDetails1 = keyPairGenerator.generateJwtSigningKeyPair();
    JWTSigningKeyDetails keyDetails2 = keyPairGenerator.generateJwtSigningKeyPair();
    assertNotEquals(keyDetails1.getPublicKeyPem(), keyDetails2.getPublicKeyPem());
  }
}
