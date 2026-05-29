package routes

import (
	"context"
	"fmt"
	"net/http"
	"qrstreamer/internal/handler"
	"qrstreamer/internal/service"
	"zaplio/shared/constant"

	"github.com/google/uuid"
)

func RegisterRoutes(hub *handler.Hub, svc service.QRStreamer) {

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), constant.CtxReqIDKey, fmt.Sprintf("%s", uuid.New().String()))
		r = r.WithContext(ctx)

		whatsappID := r.URL.Query().Get("wa_id")
		userID := r.URL.Query().Get("user_id")
		if whatsappID == "" {
			whatsappID = r.Header.Get("Whatsapp-ID")
		}
		if userID == "" {
			userID = r.Header.Get("User-ID")
		}
		if whatsappID == "" {
			http.Error(w, "Whatsapp ID is required. Use ?id=your_whatsapp_id or Whatsapp-ID header", http.StatusBadRequest)
			return
		}
		if userID == "" {
			http.Error(w, "User ID is required. Use ?user_id=your_user_id or User-ID header", http.StatusBadRequest)
			return
		}

		handler.ServeWS(hub, w, r)

		if err := svc.StreamWhatsappQR(r.Context(), userID, whatsappID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	})

	// Default root
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `WebSocket server running at ws://localhost:8080/ws`)
	})
}
