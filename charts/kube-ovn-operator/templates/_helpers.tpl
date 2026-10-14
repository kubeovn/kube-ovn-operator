{{- define "kube-ovn-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- define "kube-ovn-operator.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{ else }}{{ include "kube-ovn-operator.name" . }}{{ end }}
{{- end }}
{{- define "kube-ovn-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}{{ default (include "kube-ovn-operator.fullname" .) .Values.serviceAccount.name }}{{ else }}{{ default "default" .Values.serviceAccount.name }}{{ end }}
{{- end }}
