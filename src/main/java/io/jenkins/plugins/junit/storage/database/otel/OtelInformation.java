package io.jenkins.plugins.junit.storage.database.otel;

import hudson.EnvVars;
import io.jenkins.plugins.opentelemetry.semconv.JenkinsOtelSemanticAttributes;
import io.jenkins.plugins.opentelemetry.semconv.OTelEnvironmentVariablesConventions;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.io.Serializable;

public class OtelInformation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String traceId;
    private final String parentTraceId;
    private final String spanId;
    private final String ciPipelineId;
    private final long ciPipelineRunNumber;

    public OtelInformation(
            String traceId,
            String parentTraceId,
            String spanId,
            String ciPipelineId,
            long ciPipelineRunNumber
    ) {
        this.traceId = traceId;
        this.parentTraceId = parentTraceId;
        this.spanId = spanId;
        this.ciPipelineId = ciPipelineId;
        this.ciPipelineRunNumber = ciPipelineRunNumber;
    }

    public Attributes getAttributes() {
        return Attributes.builder()
                .put(JenkinsOtelSemanticAttributes.CI_PIPELINE_ID, ciPipelineId)
                .put(JenkinsOtelSemanticAttributes.CI_PIPELINE_RUN_NUMBER, ciPipelineRunNumber)
                .build();
    }

    public Context setup() {
        EnvVars envs = new EnvVars();
        envs.put(OTelEnvironmentVariablesConventions.TRACE_ID, traceId);
        envs.put(OTelEnvironmentVariablesConventions.SPAN_ID, spanId);
        envs.put("TRACEPARENT", parentTraceId);

        return W3CTraceContextPropagator.getInstance().extract(Context.current(), envs, new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(EnvVars envVars) {
                return envVars.keySet();
            }

            @Override
            public String get(EnvVars envVars, String s) {
                return envVars.get(s);
            }
        });
    }
}
