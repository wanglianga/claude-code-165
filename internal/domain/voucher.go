package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// ComputeVoucherHash 计算凭证哈希：prev_hash | type | payload | created_at 的 SHA-256。
// payload 使用 encoding/json 对 map 的确定性序列化（键排序），保证可重算校验。
func ComputeVoucherHash(prevHash, vtype string, payload []byte, createdAt time.Time) string {
	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write([]byte("|"))
	h.Write([]byte(vtype))
	h.Write([]byte("|"))
	h.Write(payload)
	h.Write([]byte("|"))
	h.Write([]byte(createdAt.UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(h.Sum(nil))
}

// MarshalPayload 以确定性方式序列化凭证负载。
func MarshalPayload(payload map[string]any) []byte {
	b, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// VerifyChain 重新计算整条凭证链，返回是否完整未被篡改。
func VerifyChain(vouchers []Voucher) bool {
	prev := ""
	for _, v := range vouchers {
		if v.PrevHash != prev {
			return false
		}
		payload := MarshalPayload(v.Payload)
		if ComputeVoucherHash(prev, v.Type, payload, v.CreatedAt) != v.Hash {
			return false
		}
		prev = v.Hash
	}
	return true
}
