package interceptor

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
)

// VisualizationEvent represents a single RPC event for visualization
type VisualizationEvent struct {
	Timestamp    time.Time `json:"timestamp"`
	EventID      string    `json:"event_id"`
	Service      string    `json:"service"`
	Direction    string    `json:"direction"`
	Operation    string    `json:"operation"`
	Namespace    string    `json:"namespace,omitempty"`
	WorkflowID   string    `json:"workflow_id,omitempty"`
	RunID        string    `json:"run_id,omitempty"`
	TaskQueue    string    `json:"task_queue,omitempty"`
	LatencyMs    int64     `json:"latency_ms"`
	Status       string    `json:"status"`
	ErrorMessage string    `json:"error_message,omitempty"`
}

// VisualizationInterceptor captures RPC events and broadcasts to subscribers
type VisualizationInterceptor struct {
	serviceName string
	enabled     dynamicconfig.BoolPropertyFn
	logger      log.Logger
	subscribers sync.Map // map[string]chan *VisualizationEvent
	mu          sync.RWMutex
}

// NewVisualizationInterceptor creates a new visualization interceptor
func NewVisualizationInterceptor(
	serviceName string,
	enabled dynamicconfig.BoolPropertyFn,
	logger log.Logger,
) *VisualizationInterceptor {
	return &VisualizationInterceptor{
		serviceName: serviceName,
		enabled:     enabled,
		logger:      logger,
	}
}

// Subscribe adds a new event subscriber
func (v *VisualizationInterceptor) Subscribe(id string) chan *VisualizationEvent {
	ch := make(chan *VisualizationEvent, 100) // buffered to prevent blocking
	v.subscribers.Store(id, ch)
	v.logger.Info("Visualization subscriber added", tag.NewStringTag("subscriber_id", id))
	return ch
}

// Unsubscribe removes an event subscriber
func (v *VisualizationInterceptor) Unsubscribe(id string) {
	if ch, ok := v.subscribers.LoadAndDelete(id); ok {
		close(ch.(chan *VisualizationEvent))
		v.logger.Info("Visualization subscriber removed", tag.NewStringTag("subscriber_id", id))
	}
}

// Intercept implements grpc.UnaryServerInterceptor
func (v *VisualizationInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	// Check if enabled
	if !v.enabled() {
		return handler(ctx, req)
	}

	start := time.Now()
	event := &VisualizationEvent{
		Timestamp: start,
		EventID:   generateEventID(),
		Service:   v.serviceName,
		Direction: "inbound",
		Operation: extractOperationName(info.FullMethod),
	}

	// Extract workflow context if available (basic implementation)
	v.enrichEvent(event, req)

	// Execute the actual RPC
	resp, err := handler(ctx, req)

	// Record results
	event.LatencyMs = time.Since(start).Milliseconds()
	if err != nil {
		event.Status = "error"
		event.ErrorMessage = err.Error()
	} else {
		event.Status = "success"
	}

	// Broadcast to all subscribers (non-blocking)
	v.broadcast(event)

	return resp, err
}

// broadcast sends event to all subscribers without blocking
func (v *VisualizationInterceptor) broadcast(event *VisualizationEvent) {
	v.subscribers.Range(func(key, value interface{}) bool {
		ch := value.(chan *VisualizationEvent)
		select {
		case ch <- event:
			// Event sent successfully
		default:
			// Channel full, drop event (backpressure protection)
			v.logger.Warn("Visualization event dropped - subscriber channel full",
				tag.NewStringTag("subscriber_id", key.(string)))
		}
		return true
	})
}

// enrichEvent attempts to extract workflow/namespace context from the request
func (v *VisualizationInterceptor) enrichEvent(event *VisualizationEvent, req interface{}) {
	// This is a simplified version - you could use reflection or type assertions
	// to extract more detailed information from specific request types
	
	// For now, we'll just serialize to JSON to check for common fields
	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	var reqMap map[string]interface{}
	if err := json.Unmarshal(data, &reqMap); err != nil {
		return
	}

	// Extract common fields if present
	if ns, ok := reqMap["namespace"].(string); ok {
		event.Namespace = ns
	}
	if wfID, ok := reqMap["workflow_id"].(string); ok {
		event.WorkflowID = wfID
	}
	if runID, ok := reqMap["run_id"].(string); ok {
		event.RunID = runID
	}
	if tq, ok := reqMap["task_queue"].(string); ok {
		event.TaskQueue = tq
	}

	// Check nested execution field (common in workflow requests)
	if execution, ok := reqMap["workflow_execution"].(map[string]interface{}); ok {
		if wfID, ok := execution["workflow_id"].(string); ok {
			event.WorkflowID = wfID
		}
		if runID, ok := execution["run_id"].(string); ok {
			event.RunID = runID
		}
	}
}

// extractOperationName extracts the method name from the full gRPC path
func extractOperationName(fullMethod string) string {
	// Full method format: /package.service/Method
	parts := strings.Split(fullMethod, "/")
	if len(parts) == 3 {
		return parts[2]
	}
	return fullMethod
}

// generateEventID creates a unique event identifier
func generateEventID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())
}
