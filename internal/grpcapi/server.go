// Package grpcapi 实现 gRPC 发布入口 PushService。
//
// 与 HTTP /api/push 的区别：
//   - 面向内部业务系统，适合高频、低开销的服务端调用（protobuf 编码）；
//   - 使用共享密钥 x-internal-token 鉴权，而不是用户 token；
//   - 不经过 nginx，由业务系统直连节点的 gRPC 端口。
//
// 两种入口最终都调用同一个 push.Service.Publish，保证推送语义一致。
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

// Server 实现 pb.PushServiceServer。
//
// 内嵌 UnimplementedPushServiceServer 是 gRPC-Go 的向前兼容约定：
// 以后 proto 增加新方法时，旧实现不会因为缺少方法而编译失败。
type Server struct {
	pb.UnimplementedPushServiceServer
	push          *push.Service
	internalToken string
}

// New 构造 gRPC 服务，注入发布业务层与内部共享密钥。
func New(ps *push.Service, internalToken string) *Server {
	return &Server{push: ps, internalToken: internalToken}
}

// Publish 处理一次 gRPC 发布请求。
//
// 流程：
//  1. 校验 metadata 中的 x-internal-token；
//  2. 校验 payload_json 是合法 JSON，空值按 "{}" 处理；
//  3. 调用共享的 push.Service 完成解析与投递；
//  4. 把内部结果转换成 protobuf 响应。
//
// 错误码映射：
//   - 未认证 -> Unauthenticated；
//   - 参数非法（目标/payload）-> InvalidArgument；
//   - 其他内部错误（Redis 等）-> Internal。
func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if !s.authorized(ctx) {
		return nil, status.Error(codes.Unauthenticated, "missing or invalid x-internal-token")
	}

	// payload 以字符串传输，方便调用方直接传 JSON 文本；
	// 这里先 TrimSpace，再把空值规范化为空对象。
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

// authorized 校验请求 metadata 中的 x-internal-token。
//
// 使用 subtle.ConstantTimeCompare 做常量时间比较，避免通过响应耗时
// 逐字节推断密钥。所有节点必须配置相同的 INTERNAL_TOKEN。
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
