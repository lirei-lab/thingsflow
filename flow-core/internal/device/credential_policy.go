package device

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"
)

const defaultAccessTokenMinLength = 20

func accessTokenMinLength() int {
	v := strings.TrimSpace(os.Getenv("DEVICE_ACCESS_TOKEN_MIN_LENGTH"))
	if v == "" {
		return defaultAccessTokenMinLength
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < defaultAccessTokenMinLength {
		return defaultAccessTokenMinLength
	}
	return n
}

func normalizeAccessTokenCredential(token string) (string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", nil
	}
	minLength := accessTokenMinLength()
	if len(token) < minLength {
		return "", fmt.Errorf("ACCESS_TOKEN credentialsId must be at least %d characters", minLength)
	}
	for _, r := range token {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return "", fmt.Errorf("ACCESS_TOKEN credentialsId must not contain whitespace or control characters")
		}
	}
	return token, nil
}
