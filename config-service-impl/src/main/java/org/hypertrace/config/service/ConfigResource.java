package org.hypertrace.config.service;

import lombok.Value;

/**
 * Identifies the configuration resource which you want to deal with. Multiple contexts may exist
 * for this resource, each potentially with multiple versions.
 */
@Value
public class ConfigResource {
  String resourceName;
  String resourceNamespace;
  String tenantId;
}
