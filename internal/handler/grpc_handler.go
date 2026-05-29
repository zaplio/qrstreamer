package handler

import (
	"context"
	"fmt"
	proto "zaplio/shared/proto/pb"
	"zaplio/shared/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type App struct {
	log        logger.ILogger
	grpcClient proto.WaCoreGatewayClient
	grpcConn   *grpc.ClientConn
}

type server struct {
	proto.UnimplementedWaCoreGatewayServer
}

func NewApp(log logger.ILogger) *App {
	return &App{log: log}
}

func (a *App) GRPCClient(addr string) (*grpc.ClientConn, error) {
	// Create gRPC connection with insecure credentials
	grpcConn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	// Store the gRPC client in the App struct
	a.grpcClient = proto.NewWaCoreGatewayClient(grpcConn)
	a.grpcConn = grpcConn

	return grpcConn, err
}

// GetGRPCClient returns the stored gRPC client
func (a *App) GetGRPCClient() proto.WaCoreGatewayClient {
	return a.grpcClient
}

// GetGRPCConnection returns the stored gRPC connection
func (a *App) GetGRPCConnection() *grpc.ClientConn {
	return a.grpcConn
}

// CloseGRPCConnection closes the gRPC connection
func (a *App) CloseGRPCConnection() error {
	if a.grpcConn != nil {
		return a.grpcConn.Close()
	}
	return nil
}

// IsGRPCConnected checks if gRPC client is available
func (a *App) IsGRPCConnected() bool {
	return a.grpcClient != nil && a.grpcConn != nil
}

// Example method to stream connect device
func (a *App) StreamConnectDevice(ctx context.Context, connectRequest *proto.ConnectDeviceRequest) (proto.WaCoreGateway_StreamConnectDeviceClient, error) {
	if !a.IsGRPCConnected() {
		return nil, fmt.Errorf("gRPC client not connected")
	}

	stream, err := a.grpcClient.StreamConnectDevice(ctx, connectRequest)
	return stream, err
}
