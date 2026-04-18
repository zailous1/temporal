package frontend

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for local development
		// In production, you'd want to restrict this
		return true
	},
}

// VisualizationWebSocketServer manages WebSocket connections for visualization
type VisualizationWebSocketServer struct {
	interceptor *interceptor.VisualizationInterceptor
	logger      log.Logger
}

// NewVisualizationWebSocketServer creates a new WebSocket server
func NewVisualizationWebSocketServer(
	interceptor *interceptor.VisualizationInterceptor,
	logger log.Logger,
) *VisualizationWebSocketServer {
	return &VisualizationWebSocketServer{
		interceptor: interceptor,
		logger:      logger,
	}
}

// HandleWebSocket handles WebSocket upgrade and event streaming
func (v *VisualizationWebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		v.logger.Error("Failed to upgrade WebSocket connection", tag.Error(err))
		return
	}
	defer conn.Close()

	clientID := uuid.New().String()
	v.logger.Info("New visualization client connected", tag.NewStringTag("client_id", clientID))

	// Subscribe to events
	eventChan := v.interceptor.Subscribe(clientID)
	defer v.interceptor.Unsubscribe(clientID)

	// Stream events to client
	for event := range eventChan {
		data, err := json.Marshal(event)
		if err != nil {
			v.logger.Error("Failed to marshal event", tag.Error(err))
			continue
		}

		err = conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			v.logger.Warn("Failed to write to WebSocket, client disconnected",
				tag.NewStringTag("client_id", clientID),
				tag.Error(err))
			return
		}
	}

	v.logger.Info("Visualization client disconnected", tag.NewStringTag("client_id", clientID))
}
