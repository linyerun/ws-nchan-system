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

var ErrInvalidToken = errors.New("invalid token")

var uidPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

func ValidUID(uid string) bool { return uidPattern.MatchString(uid) }

// Sign produces "uid.exp.sig". The signature binds the identity to an expiry,
// so the token is stateless and needs no server-side lookup.
func Sign(secret, uid string, ttl time.Duration) (string, int64) {
	exp := time.Now().Add(ttl).Unix()
	sig := sign(secret, uid, exp)
	return fmt.Sprintf("%s.%d.%s", uid, exp, sig), exp
}

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

func sign(secret, uid string, exp int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s.%d", uid, exp)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
