package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"hash"
	"io"
	"crypto/ecdsa"
	"crypto/md5"
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
// ECDSA Msg Signatrue
func Signature(privatekey *ecdsa.PrivateKey, msg string) (*big.Int, *big.Int, []byte, error) {
	var h hash.Hash
	h = md5.New()
	r := big.NewInt(0)
	s := big.NewInt(0)

	io.WriteString(h, msg)
	signhash := h.Sum(nil)
	r, s, err := ecdsa.Sign(rand.Reader, privatekey, signhash)

	if err != nil {
		return nil,nil, nil, err
	}

	signature := r.Bytes()
	signature = append(signature, s.Bytes()...)

	return r, s, signature, nil
}
// ECDSA Msg Signatrue Verify
func VerifySignature(pubkey ecdsa.PublicKey, r, s *big.Int, msg string) bool{

	var h hash.Hash
	h = md5.New()
	io.WriteString(h, msg)
	signhash := h.Sum(nil)

	verifystatus := ecdsa.Verify(&pubkey, signhash, r, s)
	return verifystatus

}