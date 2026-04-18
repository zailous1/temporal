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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// VisualizationEvent represents a single RPC event for visualization
type VisualizationEvent struct {
	// Core fields (always present)
	Timestamp    time.Time `json:"timestamp"`
	EventID      string    `json:"event_id"`
	Service      string    `json:"service"`
	Direction    string    `json:"direction"`
	Operation    string    `json:"operation"`
	LatencyMs    int64     `json:"latency_ms"`
	Status       string    `json:"status"`
	ErrorMessage string    `json:"error_message,omitempty"`

	// Workflow/Activity context (sometimes present)
	Namespace   string `json:"namespace,omitempty"`
	WorkflowID  string `json:"workflow_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	TaskQueue   string `json:"task_queue,omitempty"`
	WorkflowType string `json:"workflow_type,omitempty"`
	ActivityType string `json:"activity_type,omitempty"`

	// Task details
	TaskToken        string `json:"task_token,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	ScheduledEventID int64  `json:"scheduled_event_id,omitempty"`
	StartedEventID   int64  `json:"started_event_id,omitempty"`
	AttemptNumber    int32  `json:"attempt_number,omitempty"`
	RetryState       string `json:"retry_state,omitempty"`

	// Timeout configurations
	ScheduleToCloseTimeoutMs int64 `json:"schedule_to_close_timeout_ms,omitempty"`
	ScheduleToStartTimeoutMs int64 `json:"schedule_to_start_timeout_ms,omitempty"`
	StartToCloseTimeoutMs    int64 `json:"start_to_close_timeout_ms,omitempty"`
	HeartbeatTimeoutMs       int64 `json:"heartbeat_timeout_ms,omitempty"`

	// Workflow execution details
	WorkflowTaskTimeoutMs     int64  `json:"workflow_task_timeout_ms,omitempty"`
	WorkflowExecutionTimeoutMs int64 `json:"workflow_execution_timeout_ms,omitempty"`
	WorkflowRunTimeoutMs      int64  `json:"workflow_run_timeout_ms,omitempty"`
	Memo                      string `json:"memo,omitempty"`
	SearchAttributes          string `json:"search_attributes,omitempty"`

	// Metadata
	Identity         string `json:"identity,omitempty"`
	ClientSDK        string `json:"client_sdk,omitempty"`
	RequestSizeBytes int    `json:"request_size_bytes,omitempty"`
	ResponseSizeBytes int   `json:"response_size_bytes,omitempty"`

	// Payloads (can be large)
	RequestPayload  string `json:"request_payload,omitempty"`
	ResponsePayload string `json:"response_payload,omitempty"`
}

// VisualizationInterceptor captures RPC events and broadcasts to subscribers
type VisualizationInterceptor struct {
	serviceName string
	enabled     dynamicconfig.BoolPropertyFn
	logger      log.Logger
	subscribers sync.Map // map[string]chan *VisualizationEvent
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

	// Capture request payload and size
	event.RequestSizeBytes = estimateSize(req)
	event.RequestPayload = capturePayload(req, 5000) // 5KB limit

	// Extract workflow context and metadata
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
		
		// Capture response payload and size
		if resp != nil {
			event.ResponseSizeBytes = estimateSize(resp)
			event.ResponsePayload = capturePayload(resp, 5000) // 5KB limit
		}
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

// enrichEvent extracts detailed information from the request
func (v *VisualizationInterceptor) enrichEvent(event *VisualizationEvent, req interface{}) {
	// Try to extract common fields using JSON serialization
	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	var reqMap map[string]interface{}
	if err := json.Unmarshal(data, &reqMap); err != nil {
		return
	}

	// Extract common fields
	if ns, ok := reqMap["namespace"].(string); ok {
		event.Namespace = ns
	}
	if wfID, ok := reqMap["workflow_id"].(string); ok {
		event.WorkflowID = wfID
	}
	if runID, ok := reqMap["run_id"].(string); ok {
		event.RunID = runID
	}
	if identity, ok := reqMap["identity"].(string); ok {
		event.Identity = identity
	}

	// Extract task queue
	if tq, ok := reqMap["task_queue"].(map[string]interface{}); ok {
		if name, ok := tq["name"].(string); ok {
			event.TaskQueue = name
		}
	}

	// Extract workflow execution info
	if execution, ok := reqMap["workflow_execution"].(map[string]interface{}); ok {
		if wfID, ok := execution["workflow_id"].(string); ok {
			event.WorkflowID = wfID
		}
		if runID, ok := execution["run_id"].(string); ok {
			event.RunID = runID
		}
	}

	// Extract workflow type
	if wfType, ok := reqMap["workflow_type"].(map[string]interface{}); ok {
		if name, ok := wfType["name"].(string); ok {
			event.WorkflowType = name
		}
	}

	// Extract activity type
	if actType, ok := reqMap["activity_type"].(map[string]interface{}); ok {
		if name, ok := actType["name"].(string); ok {
			event.ActivityType = name
		}
	}

	// Extract task token (base64 encoded)
	if token, ok := reqMap["task_token"].(string); ok {
		event.TaskToken = token
	}

	// Extract attempt number
	if attempt, ok := reqMap["attempt"].(float64); ok {
		event.AttemptNumber = int32(attempt)
	}

	// Extract scheduled event ID
	if schedID, ok := reqMap["scheduled_event_id"].(float64); ok {
		event.ScheduledEventID = int64(schedID)
	}

	// Extract started event ID
	if startID, ok := reqMap["started_event_id"].(float64); ok {
		event.StartedEventID = int64(startID)
	}

	// Extract timeouts (convert seconds to milliseconds)
	if timeout, ok := reqMap["schedule_to_close_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.ScheduleToCloseTimeoutMs = int64(seconds * 1000)
		}
	}
	if timeout, ok := reqMap["schedule_to_start_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.ScheduleToStartTimeoutMs = int64(seconds * 1000)
		}
	}
	if timeout, ok := reqMap["start_to_close_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.StartToCloseTimeoutMs = int64(seconds * 1000)
		}
	}
	if timeout, ok := reqMap["heartbeat_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.HeartbeatTimeoutMs = int64(seconds * 1000)
		}
	}

	// Extract workflow execution timeout
	if timeout, ok := reqMap["workflow_execution_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.WorkflowExecutionTimeoutMs = int64(seconds * 1000)
		}
	}
	if timeout, ok := reqMap["workflow_run_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.WorkflowRunTimeoutMs = int64(seconds * 1000)
		}
	}
	if timeout, ok := reqMap["workflow_task_timeout"].(map[string]interface{}); ok {
		if seconds, ok := timeout["seconds"].(float64); ok {
			event.WorkflowTaskTimeoutMs = int64(seconds * 1000)
		}
	}

	// Extract memo
	if memo, ok := reqMap["memo"].(map[string]interface{}); ok {
		if memoJSON, err := json.Marshal(memo); err == nil {
			event.Memo = string(memoJSON)
		}
	}

	// Extract search attributes
	if attrs, ok := reqMap["search_attributes"].(map[string]interface{}); ok {
		if attrsJSON, err := json.Marshal(attrs); err == nil {
			event.SearchAttributes = string(attrsJSON)
		}
	}

	// Extract retry state
	if state, ok := reqMap["retry_state"].(string); ok {
		event.RetryState = state
	}
}

// capturePayload serializes a protobuf message to JSON with size limit
func capturePayload(msg interface{}, maxBytes int) string {
	// Try to convert to proto.Message
	if protoMsg, ok := msg.(proto.Message); ok {
		jsonBytes, err := protojson.Marshal(protoMsg)
		if err != nil {
			return fmt.Sprintf("[Error serializing: %v]", err)
		}
		
		// Truncate if too large
		if len(jsonBytes) > maxBytes {
			return string(jsonBytes[:maxBytes]) + "... [TRUNCATED]"
		}
		return string(jsonBytes)
	}

	// Fallback to regular JSON
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("[Error serializing: %v]", err)
	}

	// Truncate if too large
	if len(jsonBytes) > maxBytes {
		return string(jsonBytes[:maxBytes]) + "... [TRUNCATED]"
	}
	return string(jsonBytes)
}

// estimateSize estimates the size of a message in bytes
func estimateSize(msg interface{}) int {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return 0
	}
	return len(jsonBytes)
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
