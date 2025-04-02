package proxy

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/flashbots/go-utils/cli"
)

var (
	HTTPDefaultReadTimeout  = time.Duration(cli.GetEnvInt("HTTP_READ_TIMEOUT_SEC", 60)) * time.Second
	HTTPDefaultWriteTimeout = time.Duration(cli.GetEnvInt("HTTP_WRITE_TIMEOUT_SEC", 30)) * time.Second
	HTTPDefaultIdleTimeout  = time.Duration(cli.GetEnvInt("HTTP_IDLE_TIMEOUT_SEC", 3600)) * time.Second
)

type ReceiverProxyServers struct {
	proxy        *ReceiverProxy
	userServer   *http.Server
	systemServer *http.Server
	certServer   *http.Server
}

func StartReceiverServers(proxy *ReceiverProxy, userListenAddress, systemListenAddress, certListenAddress string) (*ReceiverProxyServers, error) {
	userServer := &http.Server{
		Addr:         userListenAddress,
		Handler:      proxy.UserHandler,
		TLSConfig:    proxy.TLSConfig(),
		ReadTimeout:  HTTPDefaultReadTimeout,
		WriteTimeout: HTTPDefaultWriteTimeout,
		IdleTimeout:  HTTPDefaultIdleTimeout,
	}
	systemServer := &http.Server{
		Addr:         systemListenAddress,
		Handler:      proxy.SystemHandler,
		TLSConfig:    proxy.TLSConfig(),
		ReadTimeout:  HTTPDefaultReadTimeout,
		WriteTimeout: HTTPDefaultWriteTimeout,
		IdleTimeout:  HTTPDefaultIdleTimeout,
	}
	certServer := &http.Server{
		Addr:         certListenAddress,
		Handler:      proxy.CertHandler,
		ReadTimeout:  HTTPDefaultReadTimeout,
		WriteTimeout: HTTPDefaultWriteTimeout,
		IdleTimeout:  HTTPDefaultIdleTimeout,
	}

	errCh := make(chan error)

	go func() {
		if err := systemServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			err = errors.Join(errors.New("system HTTP server failed"), err)
			errCh <- err
		}
	}()
	go func() {
		if err := userServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			err = errors.Join(errors.New("user HTTP server failed"), err)
			errCh <- err
		}
	}()
	go func() {
		if err := certServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			err = errors.Join(errors.New("cert HTTP server failed"), err)
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return nil, err
	case <-time.After(time.Millisecond * 100):
	}

	go func() {
		for {
			err, more := <-errCh
			if !more {
				return
			}
			proxy.Log.Error("Error in HTTP server", slog.Any("error", err))
		}
	}()

	return &ReceiverProxyServers{
		proxy:        proxy,
		userServer:   userServer,
		systemServer: systemServer,
		certServer:   certServer,
	}, nil
}

func (s *ReceiverProxyServers) Stop() {
	_ = s.userServer.Close()
	_ = s.systemServer.Close()
	_ = s.certServer.Close()
	s.proxy.Stop()
}

type SenderProxyServers struct {
	proxy  *SenderProxy
	server *http.Server
}

func StartSenderServers(proxy *SenderProxy, listenAddress string) (*SenderProxyServers, error) {
	server := &http.Server{
		Addr:         listenAddress,
		Handler:      proxy.Handler,
		ReadTimeout:  HTTPDefaultReadTimeout,
		WriteTimeout: HTTPDefaultWriteTimeout,
	}

	errCh := make(chan error)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			err = errors.Join(errors.New("HTTP server failed"), err)
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return nil, err
	case <-time.After(time.Millisecond * 100):
	}

	go func() {
		for {
			err, more := <-errCh
			if !more {
				return
			}
			proxy.Log.Error("Error in HTTP server", slog.Any("error", err))
		}
	}()

	return &SenderProxyServers{
		proxy:  proxy,
		server: server,
	}, nil
}

func (s *SenderProxyServers) Stop() {
	_ = s.server.Close()
	s.proxy.Stop()
}
