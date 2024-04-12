package tracing

import (
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func EndSpan(s oteltrace.Span, err error) {
	if err != nil {
		s.SetStatus(codes.Error, err.Error())
	}
	s.End()
}
