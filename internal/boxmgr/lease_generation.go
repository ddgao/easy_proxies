package boxmgr

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"easy_proxies/internal/builder"
	"easy_proxies/internal/config"
	"easy_proxies/internal/lease"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
)

// BuildLeaseCandidate 创建并启动一个不监听 Legacy 端口的独立 sing-box Candidate。
// 返回的 Candidate 拥有该实例，验证失败、提升失败或排空完成时必须调用 Close。
func BuildLeaseCandidate(ctx context.Context, cfg *config.Config, generationID string) (lease.Candidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	buildResult, err := builder.BuildLeaseGeneration(cfg)
	if err != nil {
		return lease.Candidate{}, fmt.Errorf("build Lease Generation options: %w", err)
	}
	inboundRegistry := include.InboundRegistry()
	outboundRegistry := include.OutboundRegistry()
	endpointRegistry := include.EndpointRegistry()
	dnsRegistry := include.DNSTransportRegistry()
	serviceRegistry := include.ServiceRegistry()
	boxCtx := box.Context(ctx, inboundRegistry, outboundRegistry, endpointRegistry, dnsRegistry, serviceRegistry)
	instance, err := newBoxRecover(box.Options{Context: boxCtx, Options: buildResult.Options})
	if err != nil {
		return lease.Candidate{}, fmt.Errorf("create Lease Generation: %w", err)
	}
	if err := instance.Start(); err != nil {
		_ = instance.Close()
		return lease.Candidate{}, fmt.Errorf("start Lease Generation: %w", err)
	}

	nodes := make([]lease.Node, 0, len(buildResult.NodeTags))
	seen := make(map[string]struct{}, len(buildResult.NodeTags))
	for idx := range cfg.Nodes {
		key := cfg.Nodes[idx].LeaseNodeKey()
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		tag, exists := buildResult.NodeTags[key]
		if !exists {
			continue
		}
		outbound, exists := instance.Outbound().Outbound(tag)
		if !exists {
			continue
		}
		seen[key] = struct{}{}
		nodes = append(nodes, lease.Node{Key: key, Dialer: singBoxNodeDialer{outbound: outbound}})
	}
	if len(nodes) == 0 {
		_ = instance.Close()
		return lease.Candidate{}, errors.New("Lease Generation has no available node outbound")
	}
	var closeOnce sync.Once
	var closeErr error
	return lease.Candidate{
		ID:    generationID,
		Nodes: nodes,
		Close: func() error {
			closeOnce.Do(func() { closeErr = instance.Close() })
			return closeErr
		},
	}, nil
}
