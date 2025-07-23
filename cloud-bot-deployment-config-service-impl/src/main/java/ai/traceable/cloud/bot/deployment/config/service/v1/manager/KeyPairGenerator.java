package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import ai.traceable.cloud.bot.deployment.config.service.v1.EncryptedText;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails.JWTSigningAlgorithm;
import jakarta.inject.Singleton;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;

/** Utility class for generating cryptographic key pairs used for JWT signing. */
@Slf4j
@Singleton
public class KeyPairGenerator {

  private static final String DEFAULT_ALGORITHM = "RSA";
  private static final int DEFAULT_KEY_SIZE = 2048;

  // Generates a JWT signing key details object with an asymmetric key pair.
  public JWTSigningKeyDetails generateJwtSigningKeyPair() {
    try {
      // Generate the key pair
      java.security.KeyPairGenerator keyPairGenerator =
          java.security.KeyPairGenerator.getInstance(KeyPairGenerator.DEFAULT_ALGORITHM);
      keyPairGenerator.initialize(KeyPairGenerator.DEFAULT_KEY_SIZE);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      // Convert keys to PEM format
      String publicKeyPem = convertToPem(keyPair.getPublic(), "PUBLIC KEY");

      // Create and return the JWT signing key details
      return JWTSigningKeyDetails.newBuilder()
          .setEncryptedPrivateKey(
              EncryptedText.newBuilder()
                  .setKeyId("")
                  .setValue(keyPair.getPrivate().toString())
                  .build())
          .setPublicKeyPem(publicKeyPem)
          .setAlgorithm(JWTSigningAlgorithm.JWT_SIGNING_ALGORITHM_RS256)
          .build();
    } catch (NoSuchAlgorithmException e) {
      log.error(
          "Failed to generate key pair with algorithm: {}", KeyPairGenerator.DEFAULT_ALGORITHM, e);
      throw new RuntimeException("Failed to generate key pair", e);
    }
  }

  private String convertToPem(java.security.Key key, String keyType) {
    byte[] encoded = key.getEncoded();

    // Format as PEM with MIME-style base64 encoding (64 characters per line)
    StringBuilder pemBuilder = new StringBuilder();
    pemBuilder.append("-----BEGIN ").append(keyType).append("-----\n");

    // Use getMimeEncoder for automatic line breaks at 64 characters
    String base64 = Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(encoded);
    pemBuilder.append(base64).append('\n');

    pemBuilder.append("-----END ").append(keyType).append("-----");
    return pemBuilder.toString();
  }
}
