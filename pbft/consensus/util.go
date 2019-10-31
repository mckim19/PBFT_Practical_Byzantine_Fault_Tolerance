package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"crypto/ecdsa"
	"crypto/rand"
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

func Sign(privKey *ecdsa.PrivateKey, data []byte) (*big.Int, *big.Int, []byte, error) {
	r := big.NewInt(0)
	s := big.NewInt(0)
	signHash := sha256.Sum256(data)

	r, s, err := ecdsa.Sign(rand.Reader, privKey, signHash[:])

	if err != nil {
		return nil, nil, nil, err
	}

	signature := r.Bytes()
	signature = append(signature, s.Bytes()...)

	return r, s, signature, nil
}

func Verify(pubKey *ecdsa.PublicKey, r, s *big.Int, data []byte) bool {
	signHash := sha256.Sum256(data)
	return ecdsa.Verify(pubKey, signHash[:], r, s)
}
