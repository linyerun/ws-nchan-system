// Package nchan 封装对本地 Nchan 发布接口的 HTTP 调用。
//
// Nchan 的发布端点由 nginx 提供：POST /internal/pub/{channel}。
// Go 进程不直接操作浏览器 socket，所有下行消息都通过这个接口交给
// nginx 内的 Nchan，由它扇出给该 channel 的所有订阅者。
//
// 设计要点：
//   - 复用 HTTP 连接池（每主机 64 个空闲连接），避免每次发布都握手；
//   - 总超时 5s，防止 nginx 卡死时 bridge 被永久阻塞；
//   - 发布失败做一次快速重试，覆盖 nginx reload 等瞬时抖动；
//   - 使用 X-Publisher-Token 通过 nginx 侧的内网鉴权。
package nchan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client 是本地 Nchan 发布客户端，可被 bridge 的单个 goroutine 长期复用。
type Client struct {
	baseURL string // 形如 http://node1-nginx/internal/pub
	token   string // 与 nginx 配置中 INTERNAL_TOKEN 一致
	http    *http.Client
}

// New 创建客户端并配置连接池。
//
// MaxIdleConnsPerHost 调大是必要的：bridge 串行发布时连接会被反复复用，
// 默认值 2 在突发流量下会导致频繁建连。
func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		http: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

// Publish 把 body 发布到指定 Nchan channel，最多尝试 2 次。
//
// 重试之间等待 50ms，并检查 ctx 是否已取消；网络错误与非 2xx 响应都会重试。
// Nchan 发布接口本身没有幂等键，重试可能造成重复下发；消息里带有 Envelope.ID，
// 前端会按 ID 去重，因此这里的重复是可接受的。
func (c *Client) Publish(ctx context.Context, channel string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := c.publishOnce(ctx, channel, body); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// publishOnce 执行一次 HTTP POST。
//
// URL 直接拼接 channel（形如 u:{uid}），channel 名只包含合法 uid 字符，
// 不会出现需要额外转义的情况；Nginx 侧用正则捕获该路径作为频道 ID。
// 非 2xx 响应会读取最多 512 字节响应体，方便日志定位原因。
func (c *Client) publishOnce(ctx context.Context, channel string, body []byte) error {
	url := c.baseURL + "/" + channel
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Publisher-Token", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("nchan publish %s: status=%d body=%s", channel, resp.StatusCode, bytes.TrimSpace(msg))
	}
	// 必须读完响应体，连接才能回到空闲池复用。
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
