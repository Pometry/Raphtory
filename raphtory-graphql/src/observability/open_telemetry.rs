use async_graphql::{
    async_trait,
    extensions::{
        Extension, ExtensionContext, ExtensionFactory, NextExecute, NextParseQuery, NextRequest,
        NextResolve, NextSubscribe, NextValidation, ResolveInfo,
    },
    parser::types::ExecutableDocument,
    QueryPathSegment, Response, ServerError, ServerResult, ValidationResult, Value, Variables,
};
use futures_util::{stream::BoxStream, TryFutureExt};
use opentelemetry::{
    trace::{FutureExt, SpanKind, TraceContextExt, Tracer, Span},
    Context as OpenTelemetryContext, Key,
};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const KEY_SOURCE: Key = Key::from_static_str("graphql.source");
const KEY_VARIABLES: Key = Key::from_static_str("graphql.variables");
const KEY_PARENT_TYPE: Key = Key::from_static_str("graphql.parentType");
const KEY_RETURN_TYPE: Key = Key::from_static_str("graphql.returnType");
const KEY_ERROR: Key = Key::from_static_str("graphql.error");
const KEY_COMPLEXITY: Key = Key::from_static_str("graphql.complexity");
const KEY_DEPTH: Key = Key::from_static_str("graphql.depth");

/// OpenTelemetry extension
#[cfg_attr(docsrs, doc(cfg(feature = "opentelemetry")))]
pub struct OpenTelemetry<T> {
    tracer: Arc<T>,
}

struct FieldTypeSpan {
    cumulative_duration: Duration,
    instances_count: usize,
}

impl<T> OpenTelemetry<T> {
    /// Use `tracer` to create an OpenTelemetry extension.
    pub fn new(tracer: T) -> OpenTelemetry<T>
    where
        T: Tracer + Send + Sync + 'static,
        <T as Tracer>::Span: Sync + Send,
    {
        Self {
            tracer: Arc::new(tracer),
        }
    }
}

impl<T> ExtensionFactory for OpenTelemetry<T>
where
    T: Tracer + Send + Sync + 'static,
    <T as Tracer>::Span: Sync + Send,
{
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(OpenTelemetryExtension {
            tracer: self.tracer.clone(),
            aggregate_spans: Arc::new(Mutex::new(HashMap::new())), // Initialize aggregate spans
        })
    }
}

struct OpenTelemetryExtension<T> {
    tracer: Arc<T>,
    aggregate_spans: Arc<Mutex<HashMap<String, FieldTypeSpan>>>,
}

#[async_trait::async_trait]
impl<T> Extension for OpenTelemetryExtension<T>
where
    T: Tracer + Send + Sync + 'static,
    <T as Tracer>::Span: Sync + Send,
{
    async fn request(&self, ctx: &ExtensionContext<'_>, next: NextRequest<'_>) -> Response {
        next.run(ctx)
            .with_context(OpenTelemetryContext::current_with_span(
                self.tracer
                    .span_builder("request")
                    .with_kind(SpanKind::Server)
                    .start(&*self.tracer),
            ))
            .await
    }

    fn subscribe<'s>(
        &self,
        ctx: &ExtensionContext<'_>,
        stream: BoxStream<'s, Response>,
        next: NextSubscribe<'_>,
    ) -> BoxStream<'s, Response> {
        Box::pin(
            next.run(ctx, stream)
                .with_context(OpenTelemetryContext::current_with_span(
                    self.tracer
                        .span_builder("subscribe")
                        .with_kind(SpanKind::Server)
                        .start(&*self.tracer),
                )),
        )
    }

    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let attributes = vec![
            KEY_SOURCE.string(query.to_string()),
            KEY_VARIABLES.string(serde_json::to_string(variables).unwrap()),
        ];
        let span = self
            .tracer
            .span_builder("parse")
            .with_kind(SpanKind::Server)
            .with_attributes(attributes)
            .start(&*self.tracer);

        async move {
            let res = next.run(ctx, query, variables).await;
            if let Ok(doc) = &res {
                OpenTelemetryContext::current()
                    .span()
                    .set_attribute(KEY_SOURCE.string(ctx.stringify_execute_doc(doc, variables)));
            }
            res
        }
        .with_context(OpenTelemetryContext::current_with_span(span))
        .await
    }

    async fn validation(
        &self,
        ctx: &ExtensionContext<'_>,
        next: NextValidation<'_>,
    ) -> Result<ValidationResult, Vec<ServerError>> {
        let span = self
            .tracer
            .span_builder("validation")
            .with_kind(SpanKind::Server)
            .start(&*self.tracer);
        next.run(ctx)
            .with_context(OpenTelemetryContext::current_with_span(span))
            .map_ok(|res| {
                let current_cx = OpenTelemetryContext::current();
                let span = current_cx.span();
                span.set_attribute(KEY_COMPLEXITY.i64(res.complexity as i64));
                span.set_attribute(KEY_DEPTH.i64(res.depth as i64));
                res
            })
            .await
    }

    async fn execute(
        &self,
        ctx: &ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: NextExecute<'_>,
    ) -> Response {
        // Start a span for the entire execution process
        let mut span = self
            .tracer
            .span_builder("execute")
            .with_kind(SpanKind::Server)
            .start(&*self.tracer);

        // Run the execution (resolvers will be called within this)
        let result = next.run(ctx, operation_name).await;

        self.log_aggregated_spans(&mut span);

        span.end();

        result
    }

    async fn resolve(
        &self,
        ctx: &ExtensionContext<'_>,
        info: ResolveInfo<'_>,
        next: NextResolve<'_>,
    ) -> ServerResult<Option<Value>> {
        let field_type = info.return_type.to_string();

        let start_time = Instant::now();
        let result = next.run(ctx, info).await;
        let duration = start_time.elapsed();

        let mut aggregate_spans = self.aggregate_spans.lock().unwrap();
        let entry = aggregate_spans.entry(field_type).or_insert_with(|| FieldTypeSpan {
            cumulative_duration: Duration::ZERO,
            instances_count: 0,
        });
        entry.cumulative_duration += duration;
        entry.instances_count += 1;

        result
    }
}

impl<T: Tracer + Send + Sync + 'static> OpenTelemetryExtension<T> {
    pub fn log_aggregated_spans(&self, span: &mut impl Span) {
        let spans = self.aggregate_spans.lock().unwrap();
        for (field_type, field_span) in spans.iter() {
            let avg_duration = field_span.cumulative_duration / field_span.instances_count as u32;

            let avg_duration_us = avg_duration.as_micros();
            let avg_duration_ms = avg_duration.as_millis();
            let avg_duration_s = avg_duration.as_secs();

            let mut attributes = vec![
                Key::new("instances_count").i64(field_span.instances_count as i64),
            ];

            if avg_duration_s > 0 {
                attributes.push(Key::new("avg_duration_s").i64(avg_duration_s as i64));
            } else if avg_duration_ms > 0 {
                attributes.push(Key::new("avg_duration_ms").i64(avg_duration_ms as i64));
            } else {
                attributes.push(Key::new("avg_duration_μs").i64(avg_duration_us as i64));
            }

            span.add_event(format!("Average duration for {}", field_type), attributes);
        }
    }
}
