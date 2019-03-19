// copy from 莫归 aes.go
package util

import (
	"crypto/aes"
	"math"
	"crypto/cipher"
	"encoding/base64"
)

var (
	key = []byte("07F%1Pc40d@cd8^9")
	iv = []byte("20b&d97!dbab#64L")
)

func Base64Decode(encodeText []byte) ([]byte) {
	plaintText := make([]byte, base64.StdEncoding.DecodedLen(len(encodeText)))
	l, _ := base64.StdEncoding.Decode(plaintText, []byte(encodeText))
	return plaintText[:l]
}

func DecryptCfb(text []byte) ([]byte, error) {
	firstFound := true
	var eText []byte
	nextIV := make([]byte, aes.BlockSize)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(math.Ceil(float64(len(text)) / 16.0)); i++ {
		start := i * 16
		end := start + 16
		if end > len(text) {
			end = len(text)
		}
		eText = text[start:end]
		for i, e := range eText {
			nextIV[i] = e
		}
		if firstFound {
			cfb := cipher.NewCFBDecrypter(block, iv)
			cfb.XORKeyStream(eText, eText)
			firstFound = false
		} else {
			cfb := cipher.NewCFBDecrypter(block, nextIV)
			cfb.XORKeyStream(eText, eText)
		}
	}
	return text, nil

}