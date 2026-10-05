package service

import (
	"context"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/voucher"
)

type InspectionResult struct {
	InspectionID         string        `json:"inspection_id"`
	ChainOK              bool          `json:"chain_ok"`
	FiveVouchersComplete bool          `json:"five_vouchers_complete"`
	LedgerConsistent     bool          `json:"ledger_consistent"`
	Result               string        `json:"result"`
	Findings             []string      `json:"findings"`
	Vouchers             []VoucherView `json:"vouchers"`
}

type VoucherView struct {
	Seq         int            `json:"seq"`
	Type        string         `json:"voucher_type"`
	Title       string         `json:"title"`
	PayloadHash string         `json:"payload_hash"`
	PrevHash    string         `json:"prev_hash"`
	ChainHash   string         `json:"chain_hash"`
	Payload     map[string]any `json:"payload"`
	CreatedAt   time.Time      `json:"created_at"`
}

// GetVoucherChain 返回批次凭证哈希链。
func (s *Service) GetVoucherChain(ctx context.Context, batchID string) ([]VoucherView, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	rows, err := s.db.Pool.Query(ctx, `
		SELECT seq, voucher_type, title, payload, payload_hash, prev_hash, chain_hash, created_at
		FROM vouchers WHERE batch_id=$1 ORDER BY seq`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []VoucherView{}
	for rows.Next() {
		var v VoucherView
		if err := rows.Scan(&v.Seq, &v.Type, &v.Title, &v.Payload,
			&v.PayloadHash, &v.PrevHash, &v.ChainHash, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Inspect 监管抽查：
//  1. 逐节重算哈希链；
//  2. 检查来源/温控/交接/配送/发放五类凭证完整性（按批次所处阶段判定应有的凭证）；
//  3. 检查公示读模型与权威表是否一致。
func (s *Service) Inspect(ctx context.Context, batchID, inspectorID string) (*InspectionResult, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	chain, err := s.GetVoucherChain(ctx, batchID)
	if err != nil {
		return nil, err
	}
	res := &InspectionResult{Vouchers: chain, Findings: []string{}}

	// 1. 哈希链验证
	res.ChainOK = true
	prev := voucher.Genesis
	for _, v := range chain {
		if v.PrevHash != prev {
			res.ChainOK = false
			res.Findings = append(res.Findings,
				"第"+itoa(v.Seq)+"节凭证 prev_hash 与链上前节不匹配")
		}
		if !voucher.Verify(prev, v.Payload, v.ChainHash) {
			res.ChainOK = false
			res.Findings = append(res.Findings,
				"第"+itoa(v.Seq)+"节凭证链哈希重算失败，内容疑似被篡改")
		}
		prev = v.ChainHash
	}
	if len(chain) == 0 {
		res.ChainOK = false
		res.Findings = append(res.Findings, "批次没有任何凭证")
	}

	// 2. 五证完整性（按阶段）
	var status string
	if err := s.db.Pool.QueryRow(ctx,
		`SELECT status FROM donation_batches WHERE id=$1`, id).Scan(&status); err != nil {
		return nil, apperror.ErrNotFound
	}
	have := map[string]bool{}
	for _, v := range chain {
		have[v.Type] = true
	}
	required := []string{"source"}
	switch status {
	case "in_transit":
		required = []string{"source", "temperature", "handover", "delivery"}
	case "delivered":
		required = append([]string{"source", "temperature", "handover", "delivery"})
	case "distributed":
		required = []string{"source", "temperature", "handover", "delivery", "distribution"}
	case "quarantined":
		required = []string{"source"} // 变质封存可能发生在任何阶段，仅强制来源证
	}
	missing := []string{}
	for _, t := range required {
		if !have[t] {
			missing = append(missing, t)
		}
	}
	res.FiveVouchersComplete = len(missing) == 0 && have["source"] && have["temperature"] &&
		have["handover"] && have["delivery"] && have["distribution"]
	if len(missing) > 0 {
		res.Findings = append(res.Findings, "当前阶段缺失凭证: "+joinStr(missing, ","))
	}

	// 3. 公示一致性：抽查公示行关键字段与权威表
	res.LedgerConsistent = true
	var ledgerStatus, ledgerResult, batchStatus string
	var ledgerBoxes, batchBoxes int
	err = s.db.Pool.QueryRow(ctx, `
		SELECT l.status, l.result, l.total_boxes, b.status, b.total_boxes
		FROM public_ledger l JOIN donation_batches b ON b.id=l.batch_id
		WHERE l.batch_id=$1`, id).Scan(&ledgerStatus, &ledgerResult, &ledgerBoxes,
		&batchStatus, &batchBoxes)
	if err != nil {
		res.LedgerConsistent = false
		res.Findings = append(res.Findings, "公示读模型缺少该批次或无法对账（投影滞后？）")
	} else {
		if ledgerStatus != batchStatus {
			res.LedgerConsistent = false
			res.Findings = append(res.Findings,
				"公示状态("+ledgerStatus+")与权威状态("+batchStatus+")不一致")
		}
		if ledgerBoxes != batchBoxes {
			res.LedgerConsistent = false
			res.Findings = append(res.Findings, "公示箱数与权威箱数不一致")
		}
	}
	// 未解决阻断性异常也要在抽查中暴露
	var blocking int
	_ = s.db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM exception_records WHERE batch_id=$1 AND blocking AND NOT resolved`,
		id).Scan(&blocking)
	if blocking > 0 {
		res.Findings = append(res.Findings, "存在未解决的阻断性异常，食安责任链不完整")
	}

	if res.ChainOK && res.LedgerConsistent && len(missing) == 0 && blocking == 0 {
		res.Result = "pass"
	} else {
		res.Result = "defect"
	}

	var inspID string
	if err := s.db.Pool.QueryRow(ctx, `
		INSERT INTO inspections(batch_id, inspector_id, chain_ok, five_vouchers_complete,
		      ledger_consistent, result, findings, note)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id::text`,
		id, inspectorID, res.ChainOK, res.FiveVouchersComplete,
		res.LedgerConsistent, res.Result, res.Findings,
		"监管自动抽查").Scan(&inspID); err != nil {
		return nil, err
	}
	res.InspectionID = inspID
	return res, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func joinStr(xs []string, sep string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += sep
		}
		out += x
	}
	return out
}
