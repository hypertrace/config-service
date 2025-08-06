package ai.traceable.cloud.bot.deployment.config.service.v1.encryption;

import ai.traceable.cloud.bot.deployment.config.service.v1.EncryptedText;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails.JWTSigningAlgorithm;
import com.google.inject.Inject;
import jakarta.inject.Singleton;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class KeyPairGenerator {
  private static final int RSA_KEY_SIZE = 2048;
  private final java.security.KeyPairGenerator rsaKeyPairGenerator;
  private final CloudBotEncryptionConfig cloudBotEncryptionConfig;

  @Inject
  public KeyPairGenerator(CloudBotEncryptionConfig cloudBotEncryptionConfig) {
    this.cloudBotEncryptionConfig = cloudBotEncryptionConfig;
    try {
      // Initialize RSA key pair generator
      this.rsaKeyPairGenerator = java.security.KeyPairGenerator.getInstance("RSA");
      this.rsaKeyPairGenerator.initialize(RSA_KEY_SIZE);
    } catch (NoSuchAlgorithmException e) {
      log.error("Failed to initialize key pair generator", e);
      throw new RuntimeException("Failed to initialize key pair generator", e);
    }
  }

  public JWTSigningKeyDetails generateJwtSigningKeyPair() {
    try {
      // Generate the RSA key pair
      KeyPair keyPair = rsaKeyPairGenerator.generateKeyPair();

      // Convert public key to PEM format
      String publicKeyPem = convertToPem(keyPair.getPublic(), "PUBLIC KEY");

      // Convert private key to PEM format and encrypt it
      String privateKeyPem = convertToPem(keyPair.getPrivate(), "PRIVATE KEY");
      EncryptedText encryptedPrivateKey = cloudBotEncryptionConfig.encrypt(privateKeyPem);

      // Create and return the JWT signing key details
      return JWTSigningKeyDetails.newBuilder()
          .setEncryptedPrivateKey(encryptedPrivateKey)
          .setPublicKeyPem(publicKeyPem)
          .setAlgorithm(JWTSigningAlgorithm.JWT_SIGNING_ALGORITHM_RS256)
          .build();
    } catch (Exception e) {
      log.error("Failed to generate RSA key pair", e);
      throw new RuntimeException("Failed to generate RSA key pair", e);
    }
  }

  private static String convertToPem(java.security.Key key, String keyType) {
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
