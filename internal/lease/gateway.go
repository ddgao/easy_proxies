package lease

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// gatewayHandler 是 Lease Gateway 的数据面入口。
// 它只根据 Lease Token 选择已经绑定的 NodeDialer，任何拨号错误都直接返回，不执行节点切换。
type gatewayHandler struct {
	runtime *Runtime
}

func (h *gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	username, token, ok := proxyBasicAuth(r.Header.Get("Proxy-Authorization"))
	if !ok || username != gatewayUsername {
		h.runtime.recordAuthenticationFailure(false)
		w.Header().Set("Proxy-Authenticate", `Basic realm="lease-gateway"`)
		http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
		return
	}
	connection, ok := h.runtime.connectionForToken(token)
	if !ok {
		h.runtime.recordAuthenticationFailure(true)
		w.Header().Set("Proxy-Authenticate", `Basic realm="lease-gateway"`)
		http.Error(w, "invalid lease token", http.StatusProxyAuthRequired)
		return
	}
	defer connection.Release()

	if r.Method == http.MethodConnect {
		h.handleConnect(w, r, connection)
		return
	}
	h.handleHTTP(w, r, connection)
}

func proxyBasicAuth(header string) (username, password string, ok bool) {
	const prefix = "Basic "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[len(prefix):]))
	if err != nil {
		return "", "", false
	}
	username, password, ok = strings.Cut(string(decoded), ":")
	return username, password, ok
}

func (h *gatewayHandler) handleHTTP(w http.ResponseWriter, r *http.Request, connection *connectionUse) {
	requestContext, cancel := context.WithCancel(r.Context())
	defer cancel()
	if !connection.RegisterCloser(cancel) {
		http.Error(w, "invalid lease token", http.StatusProxyAuthRequired)
		return
	}
	request := r.Clone(requestContext)
	request.RequestURI = ""
	request.Header = r.Header.Clone()
	removeHopByHopHeaders(request.Header)
	request.Header.Del("Proxy-Authorization")

	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           connection.dialer.DialContext,
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	defer transport.CloseIdleConnections()
	response, err := transport.RoundTrip(request)
	if err != nil {
		connection.ReportFailure()
		http.Error(w, "upstream proxy request failed", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	removeHopByHopHeaders(response.Header)
	copyHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}

func (h *gatewayHandler) handleConnect(w http.ResponseWriter, r *http.Request, connection *connectionUse) {
	connectContext, cancel := context.WithCancel(r.Context())
	defer cancel()
	if !connection.RegisterCloser(cancel) {
		http.Error(w, "invalid lease token", http.StatusProxyAuthRequired)
		return
	}
	upstream, err := connection.dialer.DialContext(connectContext, "tcp", r.Host)
	if err != nil {
		connection.ReportFailure()
		http.Error(w, "upstream proxy connect failed", http.StatusBadGateway)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "connection hijacking unavailable", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	if err := buffered.Flush(); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	if !connection.RegisterCloser(func() {
		_ = client.Close()
		_ = upstream.Close()
	}) {
		_ = client.Close()
		_ = upstream.Close()
		return
	}

	// CONNECT 建立后两个方向必须独立转发；任一方向结束都会关闭双方，避免泄漏隧道连接。
	proxyTunnel(client, buffered.Reader, upstream)
}

func proxyTunnel(client net.Conn, buffered *bufio.Reader, upstream net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, buffered)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		done <- struct{}{}
	}()
	<-done
	_ = client.Close()
	_ = upstream.Close()
}

func removeHopByHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
