package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"qrstreamer/internal/handler"
	"qrstreamer/model"
	"qrstreamer/util"
	"time"
	"zaplio/shared/pkg/logger"

	proto "zaplio/shared/proto/pb"

	"github.com/mdp/qrterminal"
	"github.com/redis/go-redis/v9"
)

const (
	keyWaStreamPrefix = "wsstream:%s"
	waaKeyPrefix      = "waa:%s"
	keyQrPrefix       = "qr:%s"
)

type QRStreamer interface {
	StreamWhatsappQR(ctx context.Context, userID string, whatsappID string) error
}
type service struct {
	logger logger.ILogger
	hub    *handler.Hub
	app    *handler.App
	redis  *redis.Client
}

func NewService(logger logger.ILogger, hub *handler.Hub, app *handler.App, redis *redis.Client) QRStreamer {
	return &service{
		logger: logger,
		hub:    hub,
		app:    app,
		redis:  redis,
	}
}

func (s *service) StreamWhatsappQR(ctx context.Context, userID string, whatsappID string) error {
	// Cek apakah stream untuk whatsappID sudah aktif
	streamKey := fmt.Sprintf(keyWaStreamPrefix, whatsappID)

	activeStream, err := s.redis.Get(ctx, streamKey).Bool()
	if err == nil && activeStream {
		s.logger.Infofctx(logger.AppLog, ctx, "Stream for whatsappID %s is already running", whatsappID)

		qrKey := fmt.Sprintf(keyQrPrefix, whatsappID)
		if qrCode, err := s.redis.Get(ctx, qrKey).Result(); err == nil {
			message := model.WSMessage{
				MsgStatus:  true,
				Type:       "qr_code",
				WhatsappId: whatsappID,
				Data:       qrCode,
				Timestamp:  time.Now(),
			}

			if err := s.hub.EmitMessageToClient(ctx, whatsappID, message); err != nil {
				s.logger.Errorfctx(logger.AppLog, ctx, false, "Error emitting QR code: %v", err)
			}
		}

		return nil
	}

	// Tandai stream aktif di Redis dengan TTL 1 menit
	err = s.redis.Set(ctx, streamKey, true, time.Duration(util.Configuration.Cache.WSStream)*time.Second).Err()
	if err != nil {
		s.logger.Errorfctx(logger.AppLog, ctx, false, "Error set stream status in Redis: %v", err)
	}

	defer func() {
		// Close client connection
		//s.hub.CloseClientConnection(whatsappID)

		delErr := s.redis.Del(ctx, streamKey).Err()
		if delErr != nil {
			s.logger.Errorfctx(logger.AppLog, ctx, false, "Error delete stream status in Redis: %v", delErr)
		}
	}()

	// "waa:<whatsappID>"
	redisKey := fmt.Sprintf(waaKeyPrefix, whatsappID)
	_, err = s.redis.Get(ctx, redisKey).Result()
	if err != nil {
		if err == redis.Nil {
			s.logger.Errorfctx(logger.AppLog, ctx, false, "WhatsappID %s not found in Redis", whatsappID)
			s.hub.EmitMessageToClient(ctx, whatsappID, model.WSMessage{
				MsgStatus:  false,
				Type:       "error",
				WhatsappId: whatsappID,
				Data:       fmt.Errorf("WhatsappID %s not found in Redis", whatsappID).Error(),
				Timestamp:  time.Now(),
			})
			return nil
		}
		s.logger.Errorfctx(logger.AppLog, ctx, false, "Error Redis: %v", err)
		return err
	}

	req := &proto.ConnectDeviceRequest{
		Name: whatsappID,
	}
	stream, err := s.app.StreamConnectDevice(ctx, req)
	if err != nil {
		s.logger.Errorfctx(logger.AppLog, ctx, false, "Error calling GenerateNumbers: %v", err)
		return err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			s.logger.Infofctx(logger.AppLog, ctx, "Stream closed by server")
			break
		}
		if err != nil {
			s.logger.Errorfctx(logger.AppLog, ctx, false, "Error receiving stream: %v", err)
		}

		var message model.WSMessage
		switch resp.Type {
		case "qr":
			message = model.WSMessage{
				MsgStatus:  true,
				Type:       "qr_code",
				WhatsappId: whatsappID,
				Data:       resp.Qr,
				Timestamp:  time.Now(),
			}
			qrterminal.GenerateHalfBlock(resp.Qr, qrterminal.L, os.Stdout)
		case "event":
			message = model.WSMessage{
				MsgStatus:  true,
				Type:       "event_state",
				WhatsappId: whatsappID,
				Data:       resp.Desc,
				Timestamp:  time.Now(),
			}
		default:
			continue
		}

		if err := s.hub.EmitMessageToClient(ctx, whatsappID, message); err != nil {
			s.logger.Errorfctx(logger.AppLog, ctx, false, "Error emitting QR code: %v", err)
		}
	}

	return nil
}
