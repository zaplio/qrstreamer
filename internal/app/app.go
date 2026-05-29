package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"qrstreamer/internal/handler"
	"qrstreamer/internal/provider"
	"qrstreamer/internal/routes"
	"qrstreamer/internal/service"
	"qrstreamer/util"
	"syscall"
	"time"
	"zaplio/shared/constant"
	"zaplio/shared/pkg/logger"

	"google.golang.org/grpc/connectivity"
)

func Run(cfg *util.Config) {
	ctx := context.WithValue(context.Background(), constant.CtxReqIDKey, "MAIN")

	log := logger.NewLogger(logger.LoggerConfig{
		Dir:        util.Configuration.Logger.Dir,
		FileName:   util.Configuration.Logger.FileName,
		MaxBackups: util.Configuration.Logger.MaxBackups,
		MaxSize:    util.Configuration.Logger.MaxSize,
		MaxAge:     util.Configuration.Logger.MaxAge,
		Compress:   util.Configuration.Logger.Compress,
		LocalTime:  util.Configuration.Logger.LocalTime,
		Level:      util.Configuration.Logger.Level,
	})

	redis, err := provider.NewRedisConnection(ctx)
	if err != nil {
		log.Errorfctx(logger.AppLog, ctx, false, "Failed connect to Redis: %v", err)
		return
	}

	log.Infofctx(logger.AppLog, ctx, "Application started")

	app := handler.NewApp(log)
	hub := handler.NewHub(log)
	svc := service.NewService(log, hub, app, redis)

	go hub.Run()

	go func() {
		// Setup gRPC client connection
		grpcServerAddr := fmt.Sprintf("localhost:%d", cfg.Server.Port)

		log.Infofctx(logger.AppLog, ctx, "Starting gRPC client for server at %s", grpcServerAddr)
		conn, err := app.GRPCClient(grpcServerAddr)
		if err != nil {
			log.Errorfctx(logger.AppLog, ctx, false, "Failed to create gRPC connection: %v", err)
			return
		}
		defer app.CloseGRPCConnection()

		log.Infofctx(logger.AppLog, ctx, "gRPC client connected successfully to %s", grpcServerAddr)

		// Monitor connection state
		for {
			select {
			case <-ctx.Done():
				log.Infofctx(logger.AppLog, ctx, "gRPC client shutting down")
				return
			default:
				// Check connection state
				state := conn.GetState()

				switch state {
				case connectivity.Ready:
					log.Debugfctx(logger.AppLog, ctx, "gRPC connection is ready")
				case connectivity.Connecting:
					log.Infofctx(logger.AppLog, ctx, "gRPC connection is connecting...")
				case connectivity.TransientFailure:
					log.Errorfctx(logger.AppLog, ctx, false, "gRPC connection in transient failure state")
					// Wait for state change or timeout
					if !conn.WaitForStateChange(ctx, state) {
						log.Errorfctx(logger.AppLog, ctx, false, "Context cancelled while waiting for state change")
						return
					}
				case connectivity.Idle:
					log.Infofctx(logger.AppLog, ctx, "gRPC connection is idle")
				case connectivity.Shutdown:
					log.Errorfctx(logger.AppLog, ctx, false, "gRPC connection is shutdown")
					return
				}

				// Wait before next state check
				time.Sleep(10 * time.Second)
			}
		}
	}()

	go func() {
		// Start WS HTTP server
		routes.RegisterRoutes(hub, svc)
		log.Infofctx(logger.AppLog, ctx, "Websocket Server started on :%d", cfg.Websocket.Port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Websocket.Port), nil); err != nil {
			log.Errorfctx(logger.AppLog, ctx, false, "Failed to start Websocket Server: %v", err)
		}
	}()

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownCh
	log.Infofctx(logger.AppLog, ctx, "Receiving signal: %s", sig)

	func(l logger.ILogger) {
		l.Infofctx(logger.AppLog, ctx, "Successfully stop Application.")
	}(log)

}
