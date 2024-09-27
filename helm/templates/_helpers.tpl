{{- define "traceable-config-service.inject-user-group.volumes" -}}
{{- if and .Values.podSecurityContext .Values.injectUserGroup.enabled -}}
- name: traceable-config-service-etc-mount
  configMap:
    name: traceable-config-service-etc-mount
{{- end -}}
{{- end -}}

{{- define "traceable-config-service.inject-user-group.volume-mounts" -}}
{{- if and .Values.podSecurityContext .Values.injectUserGroup.enabled -}}
- name: traceable-config-service-etc-mount
  mountPath: "/etc/passwd"
  subPath: passwd
- name: traceable-config-service-etc-mount
  mountPath: "/etc/group"
  subPath: group
{{- end -}}
{{- end -}}
