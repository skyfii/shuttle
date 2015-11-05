package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/litl/shuttle/client"
	"github.com/litl/shuttle/log"
)

var (
	Registry = ServiceRegistry{
		svcs:   make(map[string]*Service),
		vhosts: make(map[string]*VirtualHost),
	}

	ErrInvalidServiceUpdate = fmt.Errorf("configuration requires a new service")
)

type Service struct {
	sync.Mutex
	Name            string
	Addr            string
	HTTPSRedirect   bool
	VirtualHosts    []string
	Backends        []*Backend
	Balance         string
	CheckInterval   int
	Fall            int
	Rise            int
	ClientTimeout   time.Duration
	ServerTimeout   time.Duration
	DialTimeout     time.Duration
	Sent            int64
	Rcvd            int64
	Errors          int64
	HTTPConns       int64
	HTTPErrors      int64
	HTTPActive      int64
	Network         string
	MaintenanceMode bool

	// Next returns the backends in priority order.
	next func() []*Backend

	// the last backend we used and the number of times we used it
	lastBackend int
	lastCount   int

	// Each Service owns it's own netowrk listener
	tcpListener net.Listener
	udpListener *net.UDPConn

	// reverse proxy for vhost routing
	httpProxy *ReverseProxy

	// Custom Pages to backend error responses
	errorPages *ErrorResponse

	// the original map of errors as loaded in by a config
	errPagesCfg map[string][]int

	// net.Dialer so we don't need to allocate one every time
	dialer *net.Dialer
}

// Stats returned about a service
type ServiceStat struct {
	Name          string        `json:"name"`
	Addr          string        `json:"address"`
	VirtualHosts  []string      `json:"virtual_hosts"`
	Backends      []BackendStat `json:"backends"`
	Balance       string        `json:"balance"`
	CheckInterval int           `json:"check_interval"`
	Fall          int           `json:"fall"`
	Rise          int           `json:"rise"`
	ClientTimeout int           `json:"client_timeout"`
	ServerTimeout int           `json:"server_timeout"`
	DialTimeout   int           `json:"connect_timeout"`
	Sent          int64         `json:"sent"`
	Rcvd          int64         `json:"received"`
	Errors        int64         `json:"errors"`
	Conns         int64         `json:"connections"`
	Active        int64         `json:"active"`
	HTTPActive    int64         `json:"http_active"`
	HTTPConns     int64         `json:"http_connections"`
	HTTPErrors    int64         `json:"http_errors"`
}

// Create a Service from a config struct
func NewService(cfg client.ServiceConfig) *Service {
	s := &Service{
		Name:            cfg.Name,
		Addr:            cfg.Addr,
		Balance:         cfg.Balance,
		CheckInterval:   cfg.CheckInterval,
		Fall:            cfg.Fall,
		Rise:            cfg.Rise,
		HTTPSRedirect:   cfg.HTTPSRedirect,
		VirtualHosts:    cfg.VirtualHosts,
		ClientTimeout:   time.Duration(cfg.ClientTimeout) * time.Millisecond,
		ServerTimeout:   time.Duration(cfg.ServerTimeout) * time.Millisecond,
		DialTimeout:     time.Duration(cfg.DialTimeout) * time.Millisecond,
		errorPages:      NewErrorResponse(cfg.ErrorPages),
		errPagesCfg:     cfg.ErrorPages,
		Network:         cfg.Network,
		MaintenanceMode: cfg.MaintenanceMode,
	}

	// TODO: insert this into the backends too
	s.dialer = &net.Dialer{
		Timeout:   s.DialTimeout,
		KeepAlive: 30 * time.Second,
	}

	// create our reverse proxy, using our load-balancing Dial method
	proxyTransport := &http.Transport{
		Dial:                s.Dial,
		MaxIdleConnsPerHost: 10,
	}
	s.httpProxy = NewReverseProxy(proxyTransport)
	s.httpProxy.FlushInterval = time.Second
	s.httpProxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
	}

	s.httpProxy.OnResponse = []ProxyCallback{logProxyRequest, s.errStats, s.errorPages.CheckResponse}

	if s.CheckInterval == 0 {
		s.CheckInterval = client.DefaultCheckInterval
	}
	if s.Rise == 0 {
		s.Rise = client.DefaultRise
	}
	if s.Fall == 0 {
		s.Fall = client.DefaultFall
	}

	if s.Network == "" {
		s.Network = client.DefaultNet
	}

	for _, b := range cfg.Backends {
		s.add(NewBackend(b))
	}

	switch cfg.Balance {
	case client.RoundRobin:
		s.next = s.roundRobin
	case client.LeastConn:
		s.next = s.leastConn
	default:
		if cfg.Balance != "" {
			log.Warnf("invalid balancing algorithm '%s'", cfg.Balance)
		}
		s.next = s.roundRobin
	}

	return s
}

// Update the running configuration.
func (s *Service) UpdateConfig(cfg client.ServiceConfig) error {
	s.Lock()
	defer s.Unlock()

	if s.ClientTimeout != time.Duration(cfg.ClientTimeout)*time.Millisecond {
		return ErrInvalidServiceUpdate
	}

	if s.Addr != "" && s.Addr != cfg.Addr {
		return ErrInvalidServiceUpdate
	}

	s.CheckInterval = cfg.CheckInterval
	s.Fall = cfg.Fall
	s.Rise = cfg.Rise
	s.ServerTimeout = time.Duration(cfg.ServerTimeout) * time.Millisecond
	s.DialTimeout = time.Duration(cfg.DialTimeout) * time.Millisecond
	s.HTTPSRedirect = cfg.HTTPSRedirect
	s.MaintenanceMode = cfg.MaintenanceMode

	if s.Balance != cfg.Balance {
		s.Balance = cfg.Balance
		switch s.Balance {
		case client.RoundRobin:
			s.next = s.roundRobin
		case client.LeastConn:
			s.next = s.leastConn
		default:
			if cfg.Balance != "" {
				log.Warnf("invalid balancing algorithm '%s'", cfg.Balance)
			}
			s.next = s.roundRobin
		}
	}

	return nil
}

func (s *Service) Stats() ServiceStat {
	s.Lock()
	defer s.Unlock()

	stats := ServiceStat{
		Name:          s.Name,
		Addr:          s.Addr,
		VirtualHosts:  s.VirtualHosts,
		Balance:       s.Balance,
		CheckInterval: s.CheckInterval,
		Fall:          s.Fall,
		Rise:          s.Rise,
		ClientTimeout: int(s.ClientTimeout / time.Millisecond),
		ServerTimeout: int(s.ServerTimeout / time.Millisecond),
		DialTimeout:   int(s.DialTimeout / time.Millisecond),
		HTTPConns:     s.HTTPConns,
		HTTPErrors:    s.HTTPErrors,
		HTTPActive:    atomic.LoadInt64(&s.HTTPActive),
		Rcvd:          atomic.LoadInt64(&s.Rcvd),
		Sent:          atomic.LoadInt64(&s.Sent),
	}

	for _, b := range s.Backends {
		stats.Backends = append(stats.Backends, b.Stats())
		stats.Sent += b.Sent
		stats.Rcvd += b.Rcvd
		stats.Errors += b.Errors
		stats.Conns += b.Conns
		stats.Active += b.Active
	}

	return stats
}

func (s *Service) Config() client.ServiceConfig {
	s.Lock()
	defer s.Unlock()
	return s.config()
}

func (s *Service) config() client.ServiceConfig {

	config := client.ServiceConfig{
		Name:            s.Name,
		Addr:            s.Addr,
		VirtualHosts:    s.VirtualHosts,
		HTTPSRedirect:   s.HTTPSRedirect,
		Balance:         s.Balance,
		CheckInterval:   s.CheckInterval,
		Fall:            s.Fall,
		Rise:            s.Rise,
		ClientTimeout:   int(s.ClientTimeout / time.Millisecond),
		ServerTimeout:   int(s.ServerTimeout / time.Millisecond),
		DialTimeout:     int(s.DialTimeout / time.Millisecond),
		ErrorPages:      s.errPagesCfg,
		Network:         s.Network,
		MaintenanceMode: s.MaintenanceMode,
	}
	for _, b := range s.Backends {
		config.Backends = append(config.Backends, b.Config())
	}

	return config
}

func (s *Service) String() string {
	return string(marshal(s.Config()))
}

func (s *Service) get(name string) *Backend {
	s.Lock()
	defer s.Unlock()

	for _, b := range s.Backends {
		if b.Name == name {
			return b
		}
	}
	return nil
}

// Add or replace a Backend in this service
func (s *Service) add(backend *Backend) {
	s.Lock()
	defer s.Unlock()

	log.Printf("Adding %s backend %s{%s} for %s at %s", backend.Network, backend.Name, backend.Addr, s.Name, s.Addr)
	backend.up = true
	backend.rwTimeout = s.ServerTimeout
	backend.dialTimeout = s.DialTimeout
	backend.checkInterval = time.Duration(s.CheckInterval) * time.Millisecond

	// We may add some allowed protocol bridging in the future, but for now just fail
	if s.Network[:3] != backend.Network[:3] {
		log.Errorf("ERROR: backend %s cannot use network '%s'", backend.Name, backend.Network)
	}

	// replace an existing backend if we have it.
	for i, b := range s.Backends {
		if b.Name == backend.Name {
			b.Stop()
			s.Backends[i] = backend
			backend.Start()
			return
		}
	}

	s.Backends = append(s.Backends, backend)

	backend.Start()
}

// Remove a Backend by name
func (s *Service) remove(name string) bool {
	s.Lock()
	defer s.Unlock()

	for i, b := range s.Backends {
		if b.Name == name {
			log.Printf("Removing %s backend %s{%s} for %s at %s", b.Network, b.Name, b.Addr, s.Name, s.Addr)
			last := len(s.Backends) - 1
			deleted := b
			s.Backends[i], s.Backends[last] = s.Backends[last], nil
			s.Backends = s.Backends[:last]
			deleted.Stop()
			return true
		}
	}
	return false
}

// Fill out and verify service
func (s *Service) start() (err error) {
	s.Lock()
	defer s.Unlock()

	if s.Backends == nil {
		s.Backends = make([]*Backend, 0)
	}

	switch s.Network {
	case "tcp", "tcp4", "tcp6":
		log.Printf("Starting TCP listener for %s on %s", s.Name, s.Addr)

		s.tcpListener, err = newTimeoutListener(s.Network, s.Addr, s.ClientTimeout)
		if err != nil {
			return err
		}

		go s.runTCP()
	case "udp", "udp4", "udp6":
		log.Printf("Starting UDP listener for %s on %s", s.Name, s.Addr)

		laddr, err := net.ResolveUDPAddr(s.Network, s.Addr)
		if err != nil {
			return err
		}
		s.udpListener, err = net.ListenUDP(s.Network, laddr)
		if err != nil {
			return err
		}

		go s.runUDP()
	default:
		return fmt.Errorf("Error: unknown network '%s'", s.Network)
	}

	return nil
}

// Start the Service's Accept loop
func (s *Service) runTCP() {
	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Temporary() {
				log.Warnln("WARN:", err)
				continue
			}
			// we must be getting shut down
			return
		}

		go s.connectTCP(conn)
	}
}

// Code for UDP connection tracking stuff
const (
	UDPConnTrackTimeout = 90 * time.Second
	UDPBufSize          = 65507
)

type connTrackKey struct {
	IPHigh uint64
	IPLow  uint64
	Port   int
}

func newConnTrackKey(addr *net.UDPAddr) *connTrackKey {
	if len(addr.IP) == net.IPv4len {
		return &connTrackKey{
			IPHigh: 0,
			IPLow:  uint64(binary.BigEndian.Uint32(addr.IP)),
			Port:   addr.Port,
		}
	}
	return &connTrackKey{
		IPHigh: binary.BigEndian.Uint64(addr.IP[:8]),
		IPLow:  binary.BigEndian.Uint64(addr.IP[8:]),
		Port:   addr.Port,
	}
}

type connTrackMap map[connTrackKey]*net.UDPConn

// UDPProxy is proxy for which handles UDP datagrams. It implements the Proxy
// interface to handle UDP traffic forwarding between the frontend and backend
// addresses.
type UDPProxy struct {
	listener       *net.UDPConn
	frontendAddr   *net.UDPAddr
	backendAddr    *net.UDPAddr
	connTrackTable connTrackMap
	connTrackLock  sync.Mutex
}

func (s *Service) replyLoop(proxy *UDPProxy, proxyConn *net.UDPConn, clientAddr *net.UDPAddr, clientKey *connTrackKey) {
	defer func() {
		proxy.connTrackLock.Lock()
		delete(proxy.connTrackTable, *clientKey)
		proxy.connTrackLock.Unlock()
		proxyConn.Close()
	}()

	readBuf := make([]byte, UDPBufSize)
	for {
		proxyConn.SetReadDeadline(time.Now().Add(UDPConnTrackTimeout))
		again:
		read, err := proxyConn.Read(readBuf)
		if err != nil {
			if err, ok := err.(*net.OpError); ok && err.Err == syscall.ECONNREFUSED {
				// This will happen if the last write failed
				// (e.g: nothing is actually listening on the
				// proxied port on the container), ignore it
				// and continue until UDPConnTrackTimeout
				// expires:
				goto again
			}
			return
		}
		for i := 0; i != read; {
			n, err := proxy.listener.WriteToUDP(readBuf[i:read], clientAddr)
			if err != nil {
				log.Errorf("ERROR: %s", err.Error())
				atomic.AddInt64(&s.Errors, 1)
				return
			} else {
				atomic.AddInt64(&s.Sent, int64(n))
				return
			}
		}
	}
}

// Close stops forwarding the traffic.
func Close(proxy *UDPProxy) {
	proxy.listener.Close()
	proxy.connTrackLock.Lock()
	defer proxy.connTrackLock.Unlock()
	for _, conn := range proxy.connTrackTable {
		conn.Close()
	}
}

func isClosedError(err error) bool {
	/* This comparison is ugly, but unfortunately, net.go doesn't export errClosing.
	 * See:
	 * http://golang.org/src/pkg/net/net.go
	 * https://code.google.com/p/go/issues/detail?id=4337
	 * https://groups.google.com/forum/#!msg/golang-nuts/0_aaCvBmOcM/SptmDyX1XJMJ
	 */
	return strings.HasSuffix(err.Error(), "use of closed network connection")
}

func (s *Service) runUDP() {
	buff := make([]byte, UDPBufSize)
	conn := s.udpListener

	// for UDP, we can proxy the data right here.
	for {
		read, from, err := conn.ReadFromUDP(buff)
		if err != nil {
			// we can't cleanly signal the Read to stop, so we have to
			// string-match this error.
			//	if err.Error() == "use of closed network connection" {
			//		// normal shutdown
			//		return
			//	} else if err, ok := err.(net.Error); ok && err.Temporary() {
			//		log.Warnf("WARN: %s", err.Error())
			//	} else {
			// unexpected error, log it before exiting
			//		log.Errorf("ERROR: %s", err.Error())
			//		atomic.AddInt64(&s.Errors, 1)
			//		return
			//	}
			if !isClosedError(err) {
				log.Errorf("Use of closed network")
				break
			}
		}

		if read == 0 {
			continue
		}

		atomic.AddInt64(&s.Rcvd, int64(read))

		backend := s.udpRoundRobin()
		if backend == nil {
			// this could produce a lot of message
			// TODO: log some %, or max rate of messages
			continue
		}

		proxy := &UDPProxy{
			listener:       conn,
			frontendAddr:   conn.LocalAddr().(*net.UDPAddr),
			backendAddr:    backend.udpAddr,
			connTrackTable: make(connTrackMap),
		}

		fromKey := newConnTrackKey(from)
		proxy.connTrackLock.Lock()
		proxyConn, hit := proxy.connTrackTable[*fromKey]
		if !hit {
			proxyConn, err = net.DialUDP("udp", nil, proxy.backendAddr)
			if err != nil {
				log.Warnf("WARN: %s", err.Error())
				proxy.connTrackLock.Unlock()
				continue
			}
			proxy.connTrackTable[*fromKey] = proxyConn
			go s.replyLoop(proxy, proxyConn, from, fromKey)
		}
		proxy.connTrackLock.Unlock()

		n, err := proxyConn.Write(buff[:read])
		if err != nil {
			log.Errorf("ERROR: %s", err.Error())
			atomic.AddInt64(&s.Errors, 1)
			break
		} else {
			atomic.AddInt64(&s.Sent, int64(n))
		}

	}
}

// Return the addresses of the current backends in the order they would be balanced
func (s *Service) NextAddrs() []string {
	backends := s.next()

	addrs := make([]string, len(backends))
	for i, b := range backends {
		addrs[i] = b.Addr
	}
	return addrs
}

// Available returns the number of backends marked as Up
func (s *Service) Available() int {
	s.Lock()
	defer s.Unlock()

	if s.MaintenanceMode {
		return 0
	}

	available := 0
	for _, b := range s.Backends {
		if b.Up() {
			available++
		}
	}
	return available
}

// Dial a backend by address.
// This way we can wrap the connection to provide our timeout settings, as well
// as hook it into the backend stats.
// We return an error if we don't have a backend which matches.
// If Dial returns an error, we wrap it in DialError, so that a ReverseProxy
// can determine if it's safe to call RoundTrip again on a new host.
func (s *Service) Dial(nw, addr string) (net.Conn, error) {
	s.Lock()

	var backend *Backend
	for _, b := range s.Backends {
		if b.Addr == addr {
			backend = b
			break
		}
	}
	s.Unlock()

	if backend == nil {
		return nil, DialError{fmt.Errorf("no backend matching %s", addr)}
	}

	srvConn, err := s.dialer.Dial(nw, backend.Addr)
	if err != nil {
		log.Errorf("ERROR: connecting to backend %s/%s: %s", s.Name, backend.Name, err)
		atomic.AddInt64(&backend.Errors, 1)
		return nil, DialError{err}
	}

	conn := &shuttleConn{
		TCPConn:   srvConn.(*net.TCPConn),
		rwTimeout: s.ServerTimeout,
		written:   &backend.Sent,
		read:      &backend.Rcvd,
		connected: &backend.HTTPActive,
	}

	atomic.AddInt64(&backend.Conns, 1)

	// NOTE: this relies on conn.Close being called, which *should* happen in
	// all cases, but may be at fault in the active count becomes skewed in
	// some error case.
	atomic.AddInt64(&backend.HTTPActive, 1)
	return conn, nil
}

func (s *Service) connectTCP(cliConn net.Conn) {
	backends := s.next()

	// Try the first backend given, but if that fails, cycle through them all
	// to make a best effort to connect the client.
	for _, b := range backends {
		srvConn, err := s.dialer.Dial(b.Network, b.Addr)
		if err != nil {
			log.Errorf("ERROR: connecting to backend %s/%s: %s", s.Name, b.Name, err)
			atomic.AddInt64(&b.Errors, 1)
			continue
		}

		b.Proxy(srvConn, cliConn)
		return
	}

	log.Errorf("ERROR: no backend for %s", s.Name)
	cliConn.Close()
}

// Stop the Service's Accept loop by closing the Listener,
// and stop all backends for this service.
func (s *Service) stop() {
	s.Lock()
	defer s.Unlock()

	log.Printf("Stopping Listener for %s on %s:%s", s.Name, s.Network, s.Addr)
	for _, backend := range s.Backends {
		backend.Stop()
	}

	switch s.Network {
	case "tcp", "tcp4", "tcp6":
		// the service may have been bad, and the listener failed
		if s.tcpListener == nil {
			return
		}

		err := s.tcpListener.Close()
		if err != nil {
			log.Println(err)
		}

	case "udp", "udp4", "udp6":
		if s.udpListener == nil {
			return
		}
		err := s.udpListener.Close()
		if err != nil {
			log.Println(err)
		}
	}

}

// Provide a ServeHTTP method for out ReverseProxy
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&s.HTTPConns, 1)
	atomic.AddInt64(&s.HTTPActive, 1)
	defer atomic.AddInt64(&s.HTTPActive, -1)

	if s.HTTPSRedirect {
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") != "https" {
			//TODO: verify RequestURI
			redirLoc := "https://" + r.Host + r.RequestURI
			http.Redirect(w, r, redirLoc, http.StatusMovedPermanently)
			return
		}
	}

	if s.MaintenanceMode {
		// TODO: Should we increment HTTPErrors here as well?
		logRequest(r, http.StatusServiceUnavailable, "", nil, 0)
		errPage := s.errorPages.Get(http.StatusServiceUnavailable)
		if errPage != nil {
			headers := w.Header()
			for key, val := range errPage.Header() {
				headers[key] = val
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		if errPage != nil {
			w.Write(errPage.Body())
		}
		return
	}

	s.httpProxy.ServeHTTP(w, r, s.NextAddrs())
}

func (s *Service) errStats(pr *ProxyRequest) bool {
	if pr.ProxyError != nil {
		atomic.AddInt64(&s.HTTPErrors, 1)
	}
	return true
}

// A net.Listener that provides a read/write timeout
type timeoutListener struct {
	*net.TCPListener
	rwTimeout time.Duration

	// these aren't reported yet, but our new counting connections need to
	// update something
	read    int64
	written int64
}

func newTimeoutListener(netw, addr string, timeout time.Duration) (net.Listener, error) {
	lAddr, err := net.ResolveTCPAddr(netw, addr)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP(netw, lAddr)
	if err != nil {
		return nil, err
	}

	tl := &timeoutListener{
		TCPListener: l,
		rwTimeout:   timeout,
	}
	return tl, nil
}

func (l *timeoutListener) Accept() (net.Conn, error) {
	conn, err := l.TCPListener.AcceptTCP()
	if err != nil {
		return nil, err
	}

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Minute)

	sc := &shuttleConn{
		TCPConn:   conn,
		rwTimeout: l.rwTimeout,
		read:      &l.read,
		written:   &l.written,
	}
	return sc, nil
}
