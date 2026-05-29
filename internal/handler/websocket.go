package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"qrstreamer/model"
	"sync"
	"time"
	"zaplio/shared/pkg/logger"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // untuk demo, production sebaiknya dibatasi
	},
}

type Client struct {
	ctx  context.Context
	id   string
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	logger     logger.ILogger
	clients    map[string]*Client // map[whatsappID]*Client
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.Mutex
}

func (h *Hub) EmitMessageToClient(ctx context.Context, whatsappID string, data model.WSMessage) error {

	msgBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	h.logger.Infofctx(logger.AppLog, ctx, "Emitting to websocket client %s", msgBytes)

	// Emit to Websocket client
	h.EmitToClient(whatsappID, msgBytes)

	return nil
}

// EmitToAll mengirim pesan ke semua client yang terhubung
func (h *Hub) EmitToAll(message []byte) {
	h.broadcast <- message
}

// EmitToClient mengirim pesan ke client tertentu berdasarkan ID
func (h *Hub) EmitToClient(whatsappID string, message []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client, exists := h.clients[whatsappID]; exists {
		select {
		case client.send <- message:
		default:
			// Client buffer penuh, disconnect client
			delete(h.clients, whatsappID)
			close(client.send)
		}
	}
}

// GetClients mengembalikan daftar semua client yang terhubung
func (h *Hub) GetClients() []*Client {
	h.mu.Lock()
	defer h.mu.Unlock()

	clients := make([]*Client, 0, len(h.clients))
	for _, client := range h.clients {
		clients = append(clients, client)
	}
	return clients
}

// GetClientByID mengembalikan client berdasarkan ID
func (h *Hub) GetClientByID(whatsappID string) (*Client, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	client, exists := h.clients[whatsappID]
	return client, exists
}

// Close client connection by client ID
func (h *Hub) CloseClientConnection(whatsappID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client, exists := h.clients[whatsappID]; exists {
		delete(h.clients, whatsappID)
		close(client.send)
		client.conn.Close()
		h.logger.Infofctx(logger.AppLog, client.ctx, "Client disconnected with ID: %s, Address: %s", client.id, client.conn.RemoteAddr())
	}
}

// GetwhatsappIDs mengembalikan daftar semua client ID yang terhubung
func (h *Hub) GetwhatsappIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	ids := make([]string, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

func NewHub(logger logger.ILogger) *Hub {
	return &Hub{
		logger:     logger,
		clients:    make(map[string]*Client),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.id] = client
			h.logger.Infofctx(logger.AppLog, client.ctx, "Client connected with ID: %s, Address: %s", client.id, client.conn.RemoteAddr())
			h.mu.Unlock()

			message := model.WSMessage{
				MsgStatus:  true,
				Type:       "ws_state",
				WhatsappId: client.id,
				Data:       "Webstream Connected to Server",
				Timestamp:  time.Now(),
			}
			h.EmitMessageToClient(client.ctx, client.id, message)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				close(client.send)
				client.conn.Close()
				h.logger.Infofctx(logger.AppLog, client.ctx, "Client disconnected with ID: %s, Address: %s", client.id, client.conn.RemoteAddr())
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.Lock()
			for _, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client.id)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (c *Client) readPump(h *Hub) {
	defer func() {
		h.unregister <- c
		c.conn.Close()
	}()
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		h.logger.Debugfctx(logger.AppLog, c.ctx, "Received: %s", message)
		h.broadcast <- message
	}
}

func (c *Client) writePump() {
	for msg := range c.send {
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
}

func ServeWS(h *Hub, w http.ResponseWriter, r *http.Request) {
	// Ambil client ID dari query parameter
	whatsappID := r.URL.Query().Get("wa_id")
	if whatsappID == "" {
		whatsappID = r.Header.Get("Whatsapp-ID")
	}

	// Cek apakah client ID sudah ada
	// h.mu.Lock()
	// if _, exists := h.clients[whatsappID]; exists {
	// 	h.mu.Unlock()
	// 	http.Error(w, "Client ID already connected", http.StatusConflict)
	// 	return
	// }
	// h.mu.Unlock()

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Errorfctx(logger.AppLog, r.Context(), false, "Upgrade error: %v", err)
		return
	}

	client := &Client{
		ctx:  r.Context(),
		id:   whatsappID,
		conn: conn,
		send: make(chan []byte, 256),
	}
	h.register <- client

	go client.readPump(h)
	go client.writePump()
}
