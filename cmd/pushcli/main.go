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
	addr := flag.String("grpc", "127.0.0.1:9090", "push node gRPC address")
	from := flag.String("from", "ops", "caller identity")
	targetType := flag.String("type", "user", "user | room | broadcast")
	targetID := flag.String("id", "", "uid or room id")
	text := flag.String("text", "hello from grpc", "message text")
	token := flag.String("token", "dev-internal-token", "internal token")
	flag.Parse()

	payload := fmt.Sprintf(`{"text":%q}`, *text)
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
