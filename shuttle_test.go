package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"
	"github.com/skyfii/shuttle/client"
	"github.com/skyfii/shuttle/log"
	. "gopkg.in/check.v1"
)

func init() {
	debug = os.Getenv("SHUTTLE_DEBUG") == "1"

	if debug {
		log.DefaultLogger.Level = log.DEBUG
	} else {
		log.DefaultLogger = log.New(ioutil.Discard, "", 0)
	}
}

// something that can wrap a gocheck.C testing.T or testing.B
// Just add more methods as we need them.
type Tester interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Assert(interface{}, Checker, ...interface{})
}

func Test(t *testing.T) { TestingT(t) }

type BasicSuite struct {
	servers []*testServer
	service *Service
}

var _ = Suite(&BasicSuite{})

// Make Setup and TearDown more generic, so we can bypass the gocheck Suite if
// needed.
func mySetup(s *BasicSuite, t Tester) {
	// start 4 possible backend servers
	for i := 0; i < 4; i++ {
		server, err := NewTestServer("127.0.0.1:0", t)
		if err != nil {
			t.Fatal(err)
		}
		s.servers = append(s.servers, server)
	}

	svcCfg := client.ServiceConfig{
		Name:          "testService",
		Addr:          "127.0.0.1:2000",
		ClientTimeout: 1000,
		ServerTimeout: 1000,
	}

	if err := Registry.AddService(svcCfg); err != nil {
		t.Fatal(err)
	}

	s.service = Registry.GetService(svcCfg.Name)
}

// shutdown our backend servers
func myTearDown(s *BasicSuite, t Tester) {
	for _, s := range s.servers {
		s.Stop()
	}

	// get rid of the servers refs too!
	s.servers = nil

	// clear global defaults in Registry
	Registry.cfg.Balance = ""
	Registry.cfg.CheckInterval = 0
	Registry.cfg.Fall = 0
	Registry.cfg.Rise = 0
	Registry.cfg.ClientTimeout = 0
	Registry.cfg.ServerTimeout = 0
	Registry.cfg.DialTimeout = 0

	err := Registry.RemoveService(s.service.Name)
	if err != nil {
		t.Fatalf("could not remove service '%s': %s", s.service.Name, err)
	}
}

func (s *BasicSuite) SetUpTest(c *C) {
	mySetup(s, c)
}

func (s *BasicSuite) TearDownTest(c *C) {
	myTearDown(s, c)
}

// Add a default backend for the next server we have running
func (s *BasicSuite) AddBackend(c Tester) {
	// get the backends via Config to use the Service's locking.
	svcCfg := s.service.Config()
	next := len(svcCfg.Backends)
	if next >= len(s.servers) {
		c.Fatal("no more servers")
	}

	name := fmt.Sprintf("backend_%d", next)
	cfg := client.BackendConfig{
		Name:      name,
		Addr:      s.servers[next].addr,
		CheckAddr: s.servers[next].addr,
	}

	s.service.add(NewBackend(cfg))
}

// Connect to address, and check response after write.
func checkResp(addr, expected string, c Tester) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		c.Fatal(err)
	}
	defer conn.Close()

	if _, err := io.WriteString(conn, "testing\n"); err != nil {
		c.Fatal(err)
	}

	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	if err != nil {
		c.Fatal(err)
	}

	resp := string(buff[:n])
	if resp == "" {
		c.Fatal("No response")
	}

	if expected != "" && resp != expected {
		c.Fatal("Expected ", expected, ", got ", resp)
	}
}

func (s *BasicSuite) TestSingleBackend(c *C) {
	s.AddBackend(c)

	checkResp(s.service.Addr, s.servers[0].addr, c)
}

func (s *BasicSuite) TestRoundRobin(c *C) {
	s.AddBackend(c)
	s.AddBackend(c)

	checkResp(s.service.Addr, s.servers[0].addr, c)
	checkResp(s.service.Addr, s.servers[1].addr, c)
	checkResp(s.service.Addr, s.servers[0].addr, c)
	checkResp(s.service.Addr, s.servers[1].addr, c)
}

func (s *BasicSuite) TestWeightedRoundRobin(c *C) {
	s.AddBackend(c)
	s.AddBackend(c)
	s.AddBackend(c)

	s.service.Backends[0].Weight = 1
	s.service.Backends[1].Weight = 2
	s.service.Backends[2].Weight = 3

	// we already checked that we connect to the correct backends,
	// so skip the tcp connection this time.

	// one from the first server
	c.Assert(s.service.next()[0].Name, Equals, "backend_0")
	// A weight of 2 should return twice
	c.Assert(s.service.next()[0].Name, Equals, "backend_1")
	c.Assert(s.service.next()[0].Name, Equals, "backend_1")
	// And a weight of 3 should return thrice
	c.Assert(s.service.next()[0].Name, Equals, "backend_2")
	c.Assert(s.service.next()[0].Name, Equals, "backend_2")
	c.Assert(s.service.next()[0].Name, Equals, "backend_2")
	// and once around or good measure
	c.Assert(s.service.next()[0].Name, Equals, "backend_0")
}

func (s *BasicSuite) TestLeastConn(c *C) {
	// replace out default service with one using LeastConn balancing
	Registry.RemoveService("testService")
	svcCfg := client.ServiceConfig{
		Name:    "testService",
		Addr:    "127.0.0.1:2223",
		Balance: "LC",
	}

	if err := Registry.AddService(svcCfg); err != nil {
		c.Fatal(err)
	}
	s.service = Registry.GetService("testService")

	s.AddBackend(c)
	s.AddBackend(c)

	// tie up 4 connections to the backends
	buff := make([]byte, 64)
	for i := 0; i < 4; i++ {
		conn, e := net.Dial("tcp", s.service.Addr)
		if e != nil {
			c.Fatal(e)
		}
		// we need to make a call on this proxy to ensure the backend
		// connection is complete.
		if _, err := io.WriteString(conn, "connect\n"); err != nil {
			c.Fatal(err)
		}

		n, err := conn.Read(buff)
		if err != nil || n == 0 {
			c.Fatal("no response from backend")
		}

		defer conn.Close()
	}

	s.AddBackend(c)

	checkResp(s.service.Addr, s.servers[2].addr, c)
	checkResp(s.service.Addr, s.servers[2].addr, c)
}

// Test health check by taking down a server from a configured backend
func (s *BasicSuite) TestFailedCheck(c *C) {
	s.service.CheckInterval = 500
	s.service.Fall = 1
	s.AddBackend(c)

	stats := s.service.Stats()
	c.Assert(stats.Backends[0].Up, Equals, true)

	// Stop the server, and see if the backend shows Down after our check
	// interval.
	s.servers[0].Stop()
	time.Sleep(800 * time.Millisecond)

	stats = s.service.Stats()
	c.Assert(stats.Backends[0].Up, Equals, false)
	c.Assert(stats.Backends[0].CheckFail, Equals, 1)

	// now try and connect to the service
	conn, err := net.Dial("tcp", s.service.Addr)
	if err != nil {
		// we should still get an initial connection
		c.Fatal(err)
	}

	b := make([]byte, 1024)
	n, err := conn.Read(b)
	if n != 0 || err != io.EOF {
		c.Fatal("connection should have been closed")
	}

	// now bring that server back up
	server, err := NewTestServer(s.servers[0].addr, c)
	if err != nil {
		c.Fatal(err)
	}
	s.servers[0] = server

	time.Sleep(800 * time.Millisecond)
	stats = s.service.Stats()
	c.Assert(stats.Backends[0].Up, Equals, true)
}

// Make sure the connection is re-dispatched when Dialing a backend fails
func (s *BasicSuite) TestConnectAny(c *C) {
	s.service.CheckInterval = 2000
	s.service.Fall = 2
	s.AddBackend(c)
	s.AddBackend(c)

	// kill the first server
	s.servers[0].Stop()

	stats := s.service.Stats()
	c.Assert(stats.Backends[0].Up, Equals, true)

	// Backend 0 still shows up, but we should get connected to backend 1
	checkResp(s.service.Addr, s.servers[1].addr, c)
}

// Update a backend in place
func (s *BasicSuite) TestUpdateBackend(c *C) {
	s.service.CheckInterval = 500
	s.service.Fall = 1
	s.AddBackend(c)

	cfg := s.service.Config()
	backendCfg := cfg.Backends[0]

	c.Assert(backendCfg.CheckAddr, Equals, backendCfg.Addr)

	backendCfg.CheckAddr = ""
	s.service.add(NewBackend(backendCfg))

	// see if the config reflects the new backend
	cfg = s.service.Config()
	c.Assert(len(cfg.Backends), Equals, 1)
	c.Assert(cfg.Backends[0].CheckAddr, Equals, "")

	// Stopping the server should not take down the backend
	// since there is no longer a Check address.
	s.servers[0].Stop()
	time.Sleep(800 * time.Millisecond)

	stats := s.service.Stats()
	c.Assert(stats.Backends[0].Up, Equals, true)
	// should have been no check failures
	c.Assert(stats.Backends[0].CheckFail, Equals, 0)
}

// Test removal of a single Backend from a service with multiple.
func (s *BasicSuite) TestRemoveBackend(c *C) {
	s.AddBackend(c)
	s.AddBackend(c)

	stats, err := Registry.ServiceStats("testService")
	if err != nil {
		c.Fatal(err)
	}

	c.Assert(len(stats.Backends), Equals, 2)

	backend1 := stats.Backends[0].Name

	err = Registry.RemoveBackend("testService", backend1)
	if err != nil {
		c.Fatal(err)
	}

	stats, err = Registry.ServiceStats("testService")
	if err != nil {
		c.Fatal(err)
	}

	c.Assert(len(stats.Backends), Equals, 1)

	_, err = Registry.BackendStats("testService", backend1)
	c.Assert(err, Equals, ErrNoBackend)
}

func (s *BasicSuite) TestInvalidUpdateService(c *C) {
	svcCfg := client.ServiceConfig{
		Name: "Update",
		Addr: "127.0.0.1:9324",
	}

	if err := Registry.AddService(svcCfg); err != nil {
		c.Fatal(err)
	}

	svc := Registry.GetService("Update")
	if svc == nil {
		c.Fatal(ErrNoService)
	}

	svcCfg.Addr = "127.0.0.1:9425"

	// Make sure we can't add the same service again
	if err := Registry.AddService(svcCfg); err == nil {
		c.Fatal(err)
	}

	// the update should fail, because it would require a new listener
	if err := Registry.UpdateService(svcCfg); err == nil {
		c.Fatal(err)
	}

	// change the addres back, and try to update ClientTimeout
	svcCfg.Addr = "127.0.0.1:9324"
	svcCfg.ClientTimeout = 1234

	// the update should fail, because it would require a new listener
	if err := Registry.UpdateService(svcCfg); err == nil {
		c.Fatal(err)
	}

	if err := Registry.RemoveService("Update"); err != nil {
		c.Fatal(err)
	}
}

// check valid service updates
func (s *BasicSuite) TestUpdateService(c *C) {
	svcCfg := client.ServiceConfig{
		Name: "Update2",
		Addr: "127.0.0.1:9324",
	}

	if err := Registry.AddService(svcCfg); err != nil {
		c.Fatal(err)
	}

	svc := Registry.GetService("Update2")
	if svc == nil {
		c.Fatal(ErrNoService)
	}

	svcCfg.ServerTimeout = 1234
	svcCfg.HTTPSRedirect = true
	svcCfg.Fall = 5
	svcCfg.Rise = 6
	svcCfg.Balance = "LC"

	// Now update the service for real
	if err := Registry.UpdateService(svcCfg); err != nil {
		c.Fatal(err)
	}

	svc = Registry.GetService("Update2")
	if svc == nil {
		c.Fatal(ErrNoService)
	}
	c.Assert(svc.ServerTimeout, Equals, 1234*time.Millisecond)
	c.Assert(svc.HTTPSRedirect, Equals, true)
	c.Assert(svc.Fall, Equals, 5)
	c.Assert(svc.Rise, Equals, 6)
	c.Assert(svc.Balance, Equals, "LC")

	if err := Registry.RemoveService("Update2"); err != nil {
		c.Fatal(err)
	}
}

// Add backends and run response tests in parallel
func (s *BasicSuite) TestParallel(c *C) {
	var wg sync.WaitGroup

	client := func(i int) {
		s.AddBackend(c)
		// do a bunch of new connections in unison
		for i := 0; i < 100; i++ {
			checkResp(s.service.Addr, "", c)
		}

		conn, err := net.Dial("tcp", s.service.Addr)
		if err != nil {
			// we should still get an initial connection
			c.Fatal(err)
		}
		defer conn.Close()

		// now do some more continuous ping-pongs with the server
		buff := make([]byte, 1024)

		for i := 0; i < 1000; i++ {
			n, err := io.WriteString(conn, "Testing testing\n")
			if err != nil || n == 0 {
				c.Fatal("couldn't write:", err)
			}

			n, err = conn.Read(buff)
			if err != nil || n == 0 {
				c.Fatal("no response:", err)
			}
		}
		wg.Done()
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go client(i)
	}

	wg.Wait()
}

type UDPSuite struct {
	servers []*udpTestServer
	service *Service
}

var _ = Suite(&UDPSuite{})

func (s *UDPSuite) SetUpTest(c *C) {
	svcCfg := client.ServiceConfig{
		Name:    "testService",
		Addr:    "127.0.0.1:11110",
		Network: "udp",
	}

	if err := Registry.AddService(svcCfg); err != nil {
		c.Fatal(err)
	}

	s.service = Registry.GetService(svcCfg.Name)
}

func (s *UDPSuite) TearDownTest(c *C) {
	for _, s := range s.servers {
		s.Stop()
	}

	// get rid of the servers refs too!
	s.servers = nil

	// clear global defaults in Registry
	Registry.cfg.Balance = ""
	Registry.cfg.CheckInterval = 0
	Registry.cfg.Fall = 0
	Registry.cfg.Rise = 0
	Registry.cfg.ClientTimeout = 0
	Registry.cfg.ServerTimeout = 0
	Registry.cfg.DialTimeout = 0

	err := Registry.RemoveService(s.service.Name)
	if err != nil {
		c.Fatalf("could not remove service '%s': %s", s.service.Name, err)
	}
}

// Add a UDP service, make sure it works, and remove it
func (s *UDPSuite) TestAddRemove(c *C) {
	bckCfg := client.BackendConfig{
		Name:    "UDPServer",
		Addr:    "127.0.0.1:11111",
		Network: "udp",
	}

	s.service.add(NewBackend(bckCfg))

	lAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	rAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:11110")
	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		c.Fatal(err)
	}

	n, err := conn.WriteToUDP([]byte("TEST"), rAddr)
	if err != nil {
		c.Fatal(err)
	}

	// try to make sure packets were delivered and read
	time.Sleep(100 * time.Millisecond)

	stats := s.service.Stats()
	c.Assert(stats.Rcvd, Equals, int64(n))

	ok := s.service.remove("UDPServer")
	c.Assert(ok, Equals, true)

	stats = s.service.Stats()
	c.Assert(len(stats.Backends), Equals, 0)

}

// Make sure UDP Services work, and check our WeightedRoundRobin since we're
// already testing it.
func (s *UDPSuite) TestWeightedRoundRobin(c *C) {
	servers := make([]*udpTestServer, 3)

	var err error
	for i, _ := range servers {
		servers[i], err = NewUDPTestServer(fmt.Sprintf("127.0.0.1:1111%d", i+1), c)
		if err != nil {
			c.Fatal(err)
		}
		bckCfg := client.BackendConfig{
			Name:    fmt.Sprintf("UDPServer%d", i+1),
			Addr:    servers[i].addr,
			Weight:  i + 1,
			Network: "udp",
		}
		s.service.add(NewBackend(bckCfg))
	}

	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	lAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	rAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:11110")

	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		c.Fatal(err)
	}

	for i := 0; i < 12; i++ {
		msg := fmt.Sprintf("TEST_%d", i)
		_, err := conn.WriteToUDP([]byte(msg), rAddr)
		if err != nil {
			c.Fatal(err)
		}
	}

	// The order that packets are delivered to the 3 servers
	time.Sleep(100 * time.Millisecond)
	rcvOrder := []int{
		0, 6, //               servers[0]
		1, 2, 7, 8, //         servers[1]
		3, 4, 5, 9, 10, 11, // servers[2]
	}

	packetNum := 0
	for _, srv := range servers {
		srv.Lock()
		for _, p := range srv.packets {
			c.Assert(string(p), Equals, fmt.Sprintf("TEST_%d", rcvOrder[packetNum]))
			packetNum++
		}
		srv.Unlock()
	}
}

// Throw a lot of packets at the proxy then count what went through
// This doesn't pass or fail, just logs how much made it to the backend.
func (s *UDPSuite) TestSpew(c *C) {
	server, err := NewUDPTestServer("127.0.0.1:11111", c)
	if err != nil {
		c.Fatal(err)
	}
	defer server.Stop()

	bckCfg := client.BackendConfig{
		Name:    "UDPServer",
		Addr:    server.addr,
		Network: "udp",
	}
	s.service.add(NewBackend(bckCfg))

	lAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	rAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:11110")

	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		c.Fatal(err)
	}

	msg := []byte("10   BYTES")
	toSend := 10000
	for i := 0; i < toSend; i++ {
		n, err := conn.WriteToUDP(msg, rAddr)
		if err != nil || n != len(msg) {
			c.Fatal(fmt.Sprintf("%d %s", n, err))
		}
	}

	// make sure everything the service received made it to the backend.
	time.Sleep(100 * time.Millisecond)
	stats := s.service.Stats()
	c.Logf("Sent %d packets", toSend)
	c.Logf("Proxied %d packets", stats.Rcvd/10)
	c.Logf("Received %d packets", server.count)
}
