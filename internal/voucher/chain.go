package voucher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const Genesis = "GENESIS"

// canonicalJSON 以确定顺序序列化，保证哈希可复现。
// 先 marshal→unmarshal 归一化（结构体字段声明序 → map 键字典序、
// 与 PostgreSQL jsonb 读回后的形态一致），再按键排序输出。
func canonicalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		panic(err)
	}
	out, err := json.Marshal(canonicalize(generic))
	if err != nil {
		panic(err)
	}
	return out
}

func canonicalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		// Go encoding/json 已按 key 排序输出 map
		for k, val := range t {
			t[k] = canonicalize(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = canonicalize(t[i])
		}
		return t
	default:
		return v
	}
}

// PayloadHash 计算凭证内容哈希。
func PayloadHash(payload map[string]any) string {
	sum := sha256.Sum256(canonicalJSON(payload))
	return hex.EncodeToString(sum[:])
}

// ChainHash = sha256(prevHash | payloadHash)
func ChainHash(prevHash, payloadHash string) string {
	sum := sha256.Sum256([]byte(prevHash + "|" + payloadHash))
	return hex.EncodeToString(sum[:])
}

// Verify 重新计算链节哈希，供监管抽查使用。
func Verify(prevHash string, payload map[string]any, chainHash string) bool {
	ph := PayloadHash(payload)
	return ChainHash(prevHash, ph) == chainHash
}
