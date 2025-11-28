package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"easy_proxies/internal/builder"
	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
	"easy_proxies/internal/outbound/pool"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
)

// stdLogger adapts standard log to monitor.Logger interface
type stdLogger struct{}

func (l *stdLogger) Info(args ...any) {
	log.Println(append([]any{"[health-check] "}, args...)...)
}

func (l *stdLogger) Warn(args ...any) {
	log.Println(append([]any{"[health-check] ⚠️ "}, args...)...)
}

// Run builds the runtime components from config and blocks until shutdown.
func Run(ctx context.Context, cfg *config.Config) error {
	// 根据模式选择代理用户名密码
	proxyUsername := cfg.Listener.Username
	proxyPassword := cfg.Listener.Password
	if cfg.Mode == "multi_port" {
		proxyUsername = cfg.MultiPort.Username
		proxyPassword = cfg.MultiPort.Password
	}

	monitorCfg := monitor.Config{
		Enabled:       cfg.ManagementEnabled(),
		Listen:        cfg.Management.Listen,
		ProbeTarget:   cfg.Management.ProbeTarget,
		Password:      cfg.Management.Password,
		ProxyUsername: proxyUsername,
		ProxyPassword: proxyPassword,
	}
	monitorMgr, err := monitor.NewManager(monitorCfg)
	if err != nil {
		return fmt.Errorf("init monitor: %w", err)
	}

	buildResult, err := builder.Build(cfg)
	if err != nil {
		return err
	}

	inboundRegistry := include.InboundRegistry()
	outboundRegistry := include.OutboundRegistry()
	pool.Register(outboundRegistry)
	endpointRegistry := include.EndpointRegistry()
	dnsRegistry := include.DNSTransportRegistry()
	serviceRegistry := include.ServiceRegistry()

	ctx = box.Context(ctx, inboundRegistry, outboundRegistry, endpointRegistry, dnsRegistry, serviceRegistry)
	ctx = monitor.ContextWith(ctx, monitorMgr)

	instance, err := box.New(box.Options{Context: ctx, Options: buildResult})
	if err != nil {
		return fmt.Errorf("create sing-box instance: %w", err)
	}
	if err := instance.Start(); err != nil {
		return fmt.Errorf("start sing-box: %w", err)
	}

	var monitorServer *monitor.Server
	if monitorCfg.Enabled {
		monitorServer = monitor.NewServer(monitorCfg, monitorMgr, log.Default())
		monitorServer.Start(ctx)
		defer monitorServer.Shutdown(context.Background())

		monitorMgr.SetLogger(&stdLogger{})
		// 启动定期健康检查（每5分钟检查一次，每个节点超时10秒）
		monitorMgr.StartPeriodicHealthCheck(5*time.Minute, 10*time.Second)
		defer monitorMgr.Stop()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case <-ctx.Done():
	case sig := <-sigCh:
		fmt.Printf("received %s, shutting down\n", sig)
	}
	return instance.Close()
}
