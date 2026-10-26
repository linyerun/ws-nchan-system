// Package auth 实现无状态的 HMAC 客户端 token 签发与校验。
//
// token 格式为 "uid.exp.sig"：
//   - uid：用户标识，同时也是 Nchan 频道后缀（u:{uid}）；
//   - exp：过期时间（Unix 秒）；
//   - sig：HMAC-SHA256(secret, "uid.exp") 的 base64url 编码。
//
// 由于签名已经把身份和过期时间绑定在一起，服务端不需要存储会话，
// 任何节点只要持有相同密钥就能独立完成校验，天然适配水平扩展。
// 注意：这是演示用的轻量方案，生产环境建议使用标准 JWT/JWS 并轮换密钥。
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidToken 表示 token 格式错误、签名不匹配或已过期。
var ErrInvalidToken = errors.New("invalid token")

// uidPattern 限制 uid/房间 ID 的字符集。限制长度与字符范围有两个目的：
//  1. 防止奇怪字符进入 Redis key 或 Nchan channel（如 ":" 会破坏命名空间）；
//  2. 前端、日志、URL query 中传递时无需复杂转义。
var uidPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// ValidUID 判断 uid（或房间 ID）是否符合命名规范。
func ValidUID(uid string) bool { return uidPattern.MatchString(uid) }

// Sign 为 uid 签发一个 token，返回 token 字符串和过期时间（Unix 秒）。
//
// 签名内容是 "uid.exp"，因此攻击者无法在不破坏签名的情况下修改身份或
// 延长有效期。token 本身不加密，uid 和 exp 对客户端可见。
func Sign(secret, uid string, ttl time.Duration) (string, int64) {
	exp := time.Now().Add(ttl).Unix()
	sig := sign(secret, uid, exp)
	return fmt.Sprintf("%s.%d.%s", uid, exp, sig), exp
}

// Verify 校验 token 并返回其中的 uid。
//
// 校验顺序：结构 -> uid 合法性 -> 过期时间 -> 签名。
// 签名比较使用 hmac.Equal（常量时间比较），避免通过响应时间差逐字节猜测签名。
// now 由调用方传入，便于测试中模拟时间流逝。
func Verify(secret, token string, now time.Time) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", ErrInvalidToken
	}
	uid, expStr, sig := parts[0], parts[1], parts[2]
	if !ValidUID(uid) {
		return "", ErrInvalidToken
	}
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return "", ErrInvalidToken
	}
	if now.Unix() > exp {
		return "", fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	want := sign(secret, uid, exp)
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return "", ErrInvalidToken
	}
	return uid, nil
}

// sign 计算 "uid.exp" 的 HMAC-SHA256 签名，并以 base64url（无 padding）编码。
// base64url 不含 '+'、'/'、'='，可以安全地放进 URL query。
func sign(secret, uid string, exp int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s.%d", uid, exp)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
