package matching

import (
	"context"
	"encoding/json"
	"fmt"
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
		return true
	},
}

// VisualizationWebSocketServer manages WebSocket connections for visualization
type VisualizationWebSocketServer struct {
	interceptor *interceptor.VisualizationInterceptor
	logger      log.Logger
	server      *http.Server
}

// NewVisualizationWebSocketServer creates a new WebSocket server
func NewVisualizationWebSocketServer(
	interceptor *interceptor.VisualizationInterceptor,
	logger log.Logger,
	port int,
) *VisualizationWebSocketServer {
	mux := http.NewServeMux()
	
	server := &VisualizationWebSocketServer{
		interceptor: interceptor,
		logger:      logger,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}
	
	mux.HandleFunc("/api/v1/visualization/stream", server.HandleWebSocket)
	
	return server
}

// Start starts the HTTP server
func (v *VisualizationWebSocketServer) Start() error {
	v.logger.Info("Starting visualization WebSocket server for matching service", 
		tag.NewStringTag("address", v.server.Addr))
	
	go func() {
		if err := v.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			v.logger.Error("Visualization WebSocket server error", tag.Error(err))
		}
	}()
	
	return nil
}

// Stop stops the HTTP server
func (v *VisualizationWebSocketServer) Stop(ctx context.Context) error {
	v.logger.Info("Stopping visualization WebSocket server for matching service")
	return v.server.Shutdown(ctx)
}

// HandleWebSocket handles WebSocket upgrade and event streaming
func (v *VisualizationWebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		v.logger.Error("Failed to upgrade WebSocket connection", tag.Error(err))
		return
	}
	defer conn.Close()

	clientID := uuid.New().String()
	v.logger.Info("New visualization client connected to matching service", tag.NewStringTag("client_id", clientID))

	eventChan := v.interceptor.Subscribe(clientID)
	defer v.interceptor.Unsubscribe(clientID)

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

	v.logger.Info("Visualization client disconnected from matching service", tag.NewStringTag("client_id", clientID))
}
