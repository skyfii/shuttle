package client

import (
	"encoding/json"
	"reflect"
	"sort"
)

const (
	// Balancing schemes
	RoundRobin = "RR"
	LeastConn  = "LC"

	// Default timeout in milliseconds for clients and server connections
	DefaultTimeout = 2000

	// Default interval in milliseconds between health checks
	DefaultCheckInterval = 5000

	// Default network connections are TCP
	DefaultNet = "tcp"

	// All RoundRobin backends are weighted, with a default of 1
	DefaultWeight = 1

	// RoundRobin is the default balancing scheme
	DefaultBalance = RoundRobin

	// Default for Fall and Rise is 2
	DefaultFall = 2
	DefaultRise = 2
)

var (
	// Status400s is a set of response codes to set an Error page for all 4xx responses.
	Status400s = []int{400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418}
	// Status500s is a set of response codes to set an Error page for all 5xx responses.
	Status500s = []int{500, 501, 502, 503, 504, 505}
)

// Config is the global configuration for all Services.
// Defaults set here can be overridden by individual services.
type Config struct {
	// Balance method
	// Valid values are "RR" for RoundRobin, the default, and "LC" for
	// LeastConnected.
	Balance string `json:"balance,omitempty"`

	// CheckInterval is in time in milliseconds between service health checks.
	CheckInterval int `json:"check_interval"`

	// Fall is the number of failed health checks before a service is marked
	// down.
	Fall int `json:"fall"`

	// Rise is the number of successful health checks before a down service is
	// marked up.
	Rise int `json:"rise"`

	// ClientTimeout is the maximum inactivity time, in milliseconds, for a
	// connection to the client before it is closed.
	ClientTimeout int `json:"client_timeout"`

	// ServerTimeout is the maximum inactivity time, in milliseconds, for a
	// connection to the backend before it is closed.
	ServerTimeout int `json:"server_timeout"`

	// DialTimeout is the timeout in milliseconds for connections to the
	// backend service, including name resolution.
	DialTimeout int `json:"connect_timeout"`

	// HTTPSRedirect when set to true, redirects non-https request to https on
	// all services. The request may either have Scheme set to 'https',  or
	// have an "X-Forwarded-Proto: https" header.
	HTTPSRedirect bool `json:"https-redirect"`

	// Services is a slice of ServiceConfig for each service. A service
	// corresponds to one listening connection, and a number of backends to
	// proxy.
	Services []ServiceConfig `json:"services"`
}

// Marshal returns an entire config as a json []byte.
func (c *Config) Marshal() []byte {
	sort.Sort(serviceSlice(c.Services))
	js, _ := json.Marshal(c)
	return js
}

// The string representation of a config is in json.
func (c *Config) String() string {
	return string(c.Marshal())
}

// BackendConfig defines the parameters unique for individual backends.
type BackendConfig struct {
	// Name must be unique for this service.
	// Used for reference and for the HTTP API.
	Name string `json:"name"`

	// Addr must in the form ip:port
	Addr string `json:"address"`

	// Network must be "tcp" or "udp".
	// Default is "tcp"
	Network string `json:"network,omitempty"`

	// CheckAddr must be in the form ip:port.
	// A TCP connect is performed against this address to determine server
	// availability. If this is empty, no checks will be performed.
	CheckAddr string `json:"check_address"`

	// Weight is always used for RoundRobin balancing. Default is 1
	Weight int `json:"weight"`
}

// return a copy of the BackendConfig with default values set
func (b BackendConfig) SetDefaults() BackendConfig {
	if b.Weight == 0 {
		b.Weight = DefaultWeight
	}
	if b.Network == "" {
		b.Network = DefaultNet
	}
	return b
}

func (b BackendConfig) Equal(other BackendConfig) bool {
	b = b.SetDefaults()
	other = other.SetDefaults()
	return b == other
}

func (b *BackendConfig) Marshal() []byte {
	js, _ := json.Marshal(b)
	return js
}

func (b *BackendConfig) String() string {
	return string(b.Marshal())
}

// keep things sorted for easy viewing and comparison
type backendSlice []BackendConfig

func (p backendSlice) Len() int           { return len(p) }
func (p backendSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p backendSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type serviceSlice []ServiceConfig

func (p serviceSlice) Len() int           { return len(p) }
func (p serviceSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p serviceSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Subset of service fields needed for configuration.
type ServiceConfig struct {
	// Name is the unique name of the service. This is used only for reference
	// and in the HTTP API.
	Name string `json:"name"`

	// Addr is the listening address for this service. Must be in the form
	// "ip:addr"
	Addr string `json:"address"`

	// Network must be "tcp" or "udp".
	// Default is "tcp"
	Network string `json:"network,omitempty"`

	// Balance method
	// Valid values are "RR" for RoundRobin, the default, and "LC" for
	// LeastConnected.
	Balance string `json:"balance,omitempty"`

	// CheckInterval is in time in milliseconds between service health checks.
	CheckInterval int `json:"check_interval"`

	// Fall is the number of failed health checks before a service is marked.
	Fall int `json:"fall"`

	// Rise is the number of successful health checks before a down service is
	// marked up.
	Rise int `json:"rise"`

	// ClientTimeout is the maximum inactivity time, in milliseconds, for a
	// connection to the client before it is closed.
	ClientTimeout int `json:"client_timeout"`

	// ServerTimeout is the maximum inactivity time, in milliseconds, for a
	// connection to the backend before it is closed.
	ServerTimeout int `json:"server_timeout"`

	// DialTimeout is the timeout in milliseconds for connections to the
	// backend service, including name resolution.
	DialTimeout int `json:"connect_timeout"`

	// HTTPSRedirect when set to true, redirects non-https request to https. The
	// request may either have Scheme set to 'https',  or have an
	// "X-Forwarded-Proto: https" header.
	HTTPSRedirect bool `json:"https-redirect"`

	// Virtualhosts is a set of virtual hostnames for which this service should
	// handle HTTP requests.
	VirtualHosts []string `json:"virtual_hosts,omitempty"`

	// ErrorPages are responses to be returned for HTTP error codes. Each page
	// is defined by a URL mapped and is mapped to a list of error codes that
	// should return the content at the URL. Error pages are retrieved ahead of
	// time if possible, and cached.
	ErrorPages map[string][]int `json:"error_pages,omitempty"`

	// Backends is a list of all servers handling connections for this service.
	Backends []BackendConfig `json:"backends,omitempty"`

	// Maintenance mode is a flag to return 503 status codes to clients
	// without visiting backends.
	MaintenanceMode bool `json:"maintenance_mode"`
}

// Return a copy  of ServiceConfig with any unset fields to their default
// values
func (s ServiceConfig) SetDefaults() ServiceConfig {
	if s.Balance == "" {
		s.Balance = DefaultBalance
	}
	if s.CheckInterval == 0 {
		s.CheckInterval = DefaultCheckInterval
	}
	if s.ClientTimeout == 0 {
		s.ClientTimeout = DefaultTimeout
	}
	if s.ServerTimeout == 0 {
		s.ServerTimeout = DefaultTimeout
	}
	if s.Rise == 0 {
		s.Rise = DefaultRise
	}
	if s.Fall == 0 {
		s.Fall = DefaultFall
	}
	if s.Network == "" {
		s.Network = DefaultNet
	}
	return s
}

// Compare a service's settings, ignoring individual backends.
func (s ServiceConfig) Equal(other ServiceConfig) bool {
	// just remove the backends and compare the rest
	s.Backends = nil
	other.Backends = nil

	s = s.SetDefaults()
	other = other.SetDefaults()

	sort.Strings(s.VirtualHosts)
	sort.Strings(s.VirtualHosts)

	// FIXME: ignoring VirtualHosts and ErrorPages equality
	return reflect.DeepEqual(s, other)
}

// Check for equality including backends
func (s ServiceConfig) DeepEqual(other ServiceConfig) bool {
	if len(s.Backends) != len(other.Backends) {
		return false
	}

	if !s.Equal(other) {
		return false
	}

	if len(s.Backends) != len(other.Backends) {
		return false
	}

	sort.Sort(backendSlice(s.Backends))
	sort.Sort(backendSlice(other.Backends))

	for i := range s.Backends {
		if !s.Backends[i].Equal(other.Backends[i]) {
			return false
		}
	}
	return true
}

func (b *ServiceConfig) Marshal() []byte {
	sort.Sort(backendSlice(b.Backends))
	js, _ := json.Marshal(b)
	return js
}

func (b *ServiceConfig) String() string {
	return string(b.Marshal())
}

// Create a new config by merging the values from the current config
// with those set in the new config
func (s ServiceConfig) Merge(cfg ServiceConfig) ServiceConfig {
	new := s

	// let's try not to change the name
	new.Name = cfg.Name

	if cfg.Addr != "" {
		new.Addr = cfg.Addr
	}
	if cfg.Network != "" {
		new.Network = cfg.Network
	}
	if cfg.Balance != "" {
		new.Balance = cfg.Balance
	}
	if cfg.CheckInterval != 0 {
		new.CheckInterval = cfg.CheckInterval
	}
	if cfg.Fall != 0 {
		new.Fall = cfg.Fall
	}
	if cfg.Rise != 0 {
		new.Rise = cfg.Rise
	}
	if cfg.ClientTimeout != 0 {
		new.ClientTimeout = cfg.ClientTimeout
	}
	if cfg.ServerTimeout != 0 {
		new.ServerTimeout = cfg.ServerTimeout
	}
	if cfg.DialTimeout != 0 {
		new.DialTimeout = cfg.DialTimeout
	}

	if cfg.VirtualHosts != nil {
		new.VirtualHosts = cfg.VirtualHosts
	}

	if cfg.ErrorPages != nil {
		new.ErrorPages = cfg.ErrorPages
	}

	if cfg.Backends != nil {
		new.Backends = cfg.Backends
	}

	new.HTTPSRedirect = cfg.HTTPSRedirect
	new.MaintenanceMode = cfg.MaintenanceMode

	return new
}
