package grpcapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"ws-nchan-system/internal/model"
	"ws-nchan-system/internal/pb"
	"ws-nchan-system/internal/push"
)

type Server struct {
	pb.UnimplementedPushServiceServer
	push          *push.Service
	internalToken string
}

func New(ps *push.Service, internalToken string) *Server {
	return &Server{push: ps, internalToken: internalToken}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if !s.authorized(ctx) {
		return nil, status.Error(codes.Unauthenticated, "missing or invalid x-internal-token")
	}

	payload := json.RawMessage(strings.TrimSpace(req.GetPayloadJson()))
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return nil, status.Error(codes.InvalidArgument, "payload_json is not valid JSON")
	}

	res, err := s.push.Publish(ctx, model.PublishInput{
		From:       req.GetFrom(),
		TargetType: req.GetTargetType(),
		TargetID:   req.GetTargetId(),
		Payload:    payload,
	})
	if err != nil {
		if errors.Is(err, push.ErrBadTarget) || errors.Is(err, push.ErrBadPayload) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.PublishResponse{
		MsgId:             res.MsgID,
		OnlineRecipients:  int32(res.Online),
		OfflineRecipients: int32(res.Offline),
	}, nil
}

func (s *Server) authorized(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get("x-internal-token")
	if len(vals) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(vals[0]), []byte(s.internalToken)) == 1
}
