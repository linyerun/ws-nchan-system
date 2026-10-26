// Command pushcli 是调用 gRPC PushService 的极简命令行客户端，
// 用于本地验证“业务系统直连节点发布消息”的路径。
//
// 示例：
//
//	go run ./cmd/pushcli -grpc 127.0.0.1:19090 \
//	  -from ops -type user -id bob -text "hello"
//
// 也可以用 Makefile：make pushcli ARGS="..."
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"ws-nchan-system/internal/pb"
)

func main() {
	// 命令行参数：默认值对应本地单节点开发环境。
	// -type 支持 user / room / broadcast；broadcast 时 -id 会被忽略。
	addr := flag.String("grpc", "127.0.0.1:9090", "push node gRPC address")
	from := flag.String("from", "ops", "caller identity")
	targetType := flag.String("type", "user", "user | room | broadcast")
	targetID := flag.String("id", "", "uid or room id")
	text := flag.String("text", "hello from grpc", "message text")
	token := flag.String("token", "dev-internal-token", "internal token")
	flag.Parse()

	// 用 %q 生成 JSON 字符串字面量，自动处理引号/换行转义，
	// 避免直接拼接导致 payload 不是合法 JSON。
	payload := fmt.Sprintf(`{"text":%q}`, *text)
	// 演示环境为明文 gRPC；生产应使用 TLS 凭据。
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 内部鉴权：把共享密钥放进 gRPC metadata，服务端从 IncomingContext 读取。
	ctx = metadata.AppendToOutgoingContext(ctx, "x-internal-token", *token)

	client := pb.NewPushServiceClient(conn)
	resp, err := client.Publish(ctx, &pb.PublishRequest{
		From:        *from,
		TargetType:  *targetType,
		TargetId:    *targetID,
		PayloadJson: payload,
	})
	if err != nil {
		fail("publish: %v", err)
	}
	fmt.Printf("msg_id=%s online=%d offline=%d\n", resp.GetMsgId(), resp.GetOnlineRecipients(), resp.GetOfflineRecipients())
}

// fail 输出错误到 stderr 并以非零状态退出，方便脚本判断成败。
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
