package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func Hash(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

func Digest(object interface{}) string {
	msg, err := json.Marshal(object)

	if err != nil {
		panic(err.Error())
	}

	return Hash(msg)
}
