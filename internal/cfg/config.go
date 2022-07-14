/*
 * Copyright (c) 2021-2022 UNNG Lab.
 */

package cfg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/chunkreader/v2"
	"github.com/jackc/pgpassfile"
	"github.com/jackc/pgservicefile"

	"pap/internal/pgproto"
)

// DialFunc is a function that can be used to connect to a PostgreSQL server.
type DialFunc func(network, addr string) (net.Conn, error)

// BuildFrontendFunc is a function that can be used to create Frontend implementation for connection.
type BuildFrontendFunc func(r io.Reader, w io.Writer) *pgproto.Frontend

// LookupFunc is a function that can be used to lookup IPs addrs from host.
type LookupFunc func(ctx context.Context, host string) (addrs []string, err error)

// TODO after + validate connect
//type AfterConnectFunc func(ctx context.Context, pgconn *PgConn) error
//type ValidateConnectFunc func(ctx context.Context, pgconn *PgConn) error

// Config is the settings used to establish a connection to a PostgreSQL server. It must be created by ParseConfig. A
// manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	Host           string // host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
	Port           uint16
	Database       string
	User           string
	Password       string
	TLSConfig      *tls.Config // nil disables TLS
	ConnectTimeout time.Duration
	DialFunc       DialFunc   // e.g. net.Dialer.DialContext
	LookupFunc     LookupFunc // e.g. net.Resolver.LookupHost
	BuildFrontend  BuildFrontendFunc
	RuntimeParams  map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)

	Fallbacks []*FallbackConfig

	// ValidateConnect is called during a connection attempt after a successful authentication with the PostgreSQL server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback config is tried. This allows implementing high availability behavior such as libpq does with target_session_attrs.
	// TODO after + validate connect
	//ValidateConnect ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	// TODO after + validate connect
	//AfterConnect AfterConnectFunc

	// OnNotice is a callback function called when a notice response is received.
	// TODO notice
	//OnNotice NoticeHandler

	// OnNotification is a callback function called when a notification from the LISTEN/NOTIFY system is received.
	// TODO notice
	//OnNotification NotificationHandler

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the TLSConfig field:
// according to the tls.Config docs it must not be modified after creation.
func (c *Config) Copy() *Config {
	newConf := new(Config)
	*newConf = *c
	if newConf.TLSConfig != nil {
		newConf.TLSConfig = c.TLSConfig.Clone()
	}
	if newConf.RuntimeParams != nil {
		newConf.RuntimeParams = make(map[string]string, len(c.RuntimeParams))
		for k, v := range c.RuntimeParams {
			newConf.RuntimeParams[k] = v
		}
	}
	if newConf.Fallbacks != nil {
		newConf.Fallbacks = make([]*FallbackConfig, len(c.Fallbacks))
		for i, fallback := range c.Fallbacks {
			newFallback := new(FallbackConfig)
			*newFallback = *fallback
			if newFallback.TLSConfig != nil {
				newFallback.TLSConfig = fallback.TLSConfig.Clone()
			}
			newConf.Fallbacks[i] = newFallback
		}
	}
	return newConf
}

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer and high availability (HA) connections.
type FallbackConfig struct {
	Host      string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port      uint16
	TLSConfig *tls.Config // nil disables TLS
}

// NetworkAddress converts a PostgreSQL host and port into network and address suitable for use with
// net.Dial.
func NetworkAddress(host string, port uint16) (network, address string) {
	if strings.HasPrefix(host, "/") {
		network = "unix"
		address = filepath.Join(host, ".s.PGSQL.") + strconv.FormatInt(int64(port), 10)
	} else {
		network = "tcp"
		address = net.JoinHostPort(host, strconv.Itoa(int(port)))
	}
	return network, address
}

func (c *Config) ParseConfig(connString string) error {
	defaultSettings := defaultSettings()
	envSettings := parseEnvSettings()

	connStringSettings := make(map[string]string)
	if connString != "" {
		var err error
		// connString may be a database URL or a DSN
		if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
			connStringSettings, err = parseURLSettings(connString)
			if err != nil {
				return &parseConfigError{connString: connString, msg: "failed to parse as URL", err: err}
			}
		} else {
			connStringSettings, err = parseDSNSettings(connString)
			if err != nil {
				return &parseConfigError{connString: connString, msg: "failed to parse as DSN", err: err}
			}
		}
	}

	settings := mergeSettings(defaultSettings, envSettings, connStringSettings)
	if service, present := settings["service"]; present {
		serviceSettings, err := parseServiceSettings(settings["servicefile"], service)
		if err != nil {
			return &parseConfigError{connString: connString, msg: "failed to read service", err: err}
		}

		settings = mergeSettings(defaultSettings, envSettings, serviceSettings, connStringSettings)
	}

	minReadBufferSize, err := strconv.ParseInt(settings["min_read_buffer_size"], 10, 32)
	if err != nil {
		return &parseConfigError{connString: connString, msg: "cannot parse min_read_buffer_size", err: err}
	}

	c.createdByParseConfig = true
	c.Database = settings["database"]
	c.User = settings["user"]
	c.Password = settings["password"]
	c.RuntimeParams = make(map[string]string)
	c.BuildFrontend = makeDefaultBuildFrontendFunc(int(minReadBufferSize))

	if connectTimeoutSetting, present := settings["connect_timeout"]; present {
		connectTimeout, err := parseConnectTimeoutSetting(connectTimeoutSetting)
		if err != nil {
			return &parseConfigError{connString: connString, msg: "invalid connect_timeout", err: err}
		}
		c.ConnectTimeout = connectTimeout
		c.DialFunc = makeConnectTimeoutDialFunc(connectTimeout)
	} else {
		defaultDialer := makeDefaultDialer()
		c.DialFunc = defaultDialer.Dial
	}

	c.LookupFunc = makeDefaultResolver().LookupHost

	notRuntimeParams := map[string]struct{}{
		"host":                 {},
		"port":                 {},
		"database":             {},
		"user":                 {},
		"password":             {},
		"passfile":             {},
		"connect_timeout":      {},
		"sslmode":              {},
		"sslkey":               {},
		"sslcert":              {},
		"sslrootcert":          {},
		"target_session_attrs": {},
		"min_read_buffer_size": {},
		"service":              {},
		"servicefile":          {},
	}

	for k, v := range settings {
		if _, present := notRuntimeParams[k]; present {
			continue
		}
		c.RuntimeParams[k] = v
	}

	fallbacks := []*FallbackConfig{}

	hosts := strings.Split(settings["host"], ",")
	ports := strings.Split(settings["port"], ",")

	for i, host := range hosts {
		var portStr string
		if i < len(ports) {
			portStr = ports[i]
		} else {
			portStr = ports[0]
		}

		port, err := parsePort(portStr)
		if err != nil {
			return &parseConfigError{connString: connString, msg: "invalid port", err: err}
		}

		var tlsConfigs []*tls.Config

		// Ignore TLS settings if Unix domain socket like libpq
		if network, _ := NetworkAddress(host, port); network == "unix" {
			tlsConfigs = append(tlsConfigs, nil)
		} else {
			var err error
			tlsConfigs, err = configTLS(settings, host)
			if err != nil {
				return &parseConfigError{connString: connString, msg: "failed to configure TLS", err: err}
			}
		}

		for _, tlsConfig := range tlsConfigs {
			fallbacks = append(fallbacks, &FallbackConfig{
				Host:      host,
				Port:      port,
				TLSConfig: tlsConfig,
			})
		}
	}

	c.Host = fallbacks[0].Host
	c.Port = fallbacks[0].Port
	c.TLSConfig = fallbacks[0].TLSConfig
	c.Fallbacks = fallbacks[1:]

	passfile, err := pgpassfile.ReadPassfile(settings["passfile"])
	if err == nil {
		if c.Password == "" {
			host := c.Host
			if network, _ := NetworkAddress(c.Host, c.Port); network == "unix" {
				host = "localhost"
			}

			c.Password = passfile.FindPassword(host, strconv.Itoa(int(c.Port)), c.Database, c.User)
		}
	}

	if settings["target_session_attrs"] == "read-write" {
		// TODO validate connect
		//config.ValidateConnect = ValidateConnectTargetSessionAttrsReadWrite
	} else if settings["target_session_attrs"] != "any" {
		return &parseConfigError{connString: connString, msg: fmt.Sprintf("unknown target_session_attrs value: %v", settings["target_session_attrs"])}
	}

	return nil
}

func mergeSettings(settingSets ...map[string]string) map[string]string {
	settings := make(map[string]string)

	for _, s2 := range settingSets {
		for k, v := range s2 {
			settings[k] = v
		}
	}

	return settings
}

func parseEnvSettings() map[string]string {
	settings := make(map[string]string)

	nameMap := map[string]string{
		"PGHOST":               "host",
		"PGPORT":               "port",
		"PGDATABASE":           "database",
		"PGUSER":               "user",
		"PGPASSWORD":           "password",
		"PGPASSFILE":           "passfile",
		"PGAPPNAME":            "application_name",
		"PGCONNECT_TIMEOUT":    "connect_timeout",
		"PGSSLMODE":            "sslmode",
		"PGSSLKEY":             "sslkey",
		"PGSSLCERT":            "sslcert",
		"PGSSLROOTCERT":        "sslrootcert",
		"PGTARGETSESSIONATTRS": "target_session_attrs",
		"PGSERVICE":            "service",
		"PGSERVICEFILE":        "servicefile",
	}

	for envname, realname := range nameMap {
		value := os.Getenv(envname)
		if value != "" {
			settings[realname] = value
		}
	}

	return settings
}

func parseURLSettings(connString string) (map[string]string, error) {
	settings := make(map[string]string)

	url, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}

	if url.User != nil {
		settings["user"] = url.User.Username()
		if password, present := url.User.Password(); present {
			settings["password"] = password
		}
	}

	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(url.Host, ",") {
		if host == "" {
			continue
		}
		if isIPOnly(host) {
			hosts = append(hosts, strings.Trim(host, "[]"))
			continue
		}
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return nil, fmt.Errorf("failed to split host:port in '%s', err: %w", host, err)
		}
		if h != "" {
			hosts = append(hosts, h)
		}
		if p != "" {
			ports = append(ports, p)
		}
	}
	if len(hosts) > 0 {
		settings["host"] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		settings["port"] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(url.Path, "/")
	if database != "" {
		settings["database"] = database
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	for k, v := range url.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		settings[k] = v[0]
	}

	return settings, nil
}

func isIPOnly(host string) bool {
	return net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":")
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func parseDSNSettings(s string) (map[string]string, error) {
	settings := make(map[string]string)

	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return nil, errors.New("invalid dsn")
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		if len(s) == 0 {
		} else if s[0] != '\'' {
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, errors.New("invalid backslash")
					}
				}
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		} else { // quoted string
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return nil, errors.New("unterminated quoted string in connection info string")
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		}

		if k, ok := nameMap[key]; ok {
			key = k
		}

		if key == "" {
			return nil, errors.New("invalid dsn")
		}

		settings[key] = val
	}

	return settings, nil
}

func parseServiceSettings(servicefilePath, serviceName string) (map[string]string, error) {
	servicefile, err := pgservicefile.ReadServicefile(servicefilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read service file: %v", servicefilePath)
	}

	service, err := servicefile.GetService(serviceName)
	if err != nil {
		return nil, fmt.Errorf("unable to find service: %v", serviceName)
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	settings := make(map[string]string, len(service.Settings))
	for k, v := range service.Settings {
		if k2, present := nameMap[k]; present {
			k = k2
		}
		settings[k] = v
	}

	return settings, nil
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
func configTLS(settings map[string]string, thisHost string) ([]*tls.Config, error) {
	host := thisHost
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]

	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}

	tlsConfig := &tls.Config{}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		// According to PostgreSQL documentation, if a root CA file exists,
		// the behavior of sslmode=require should be the same as that of verify-ca
		//
		// See https://www.postgresql.org/docs/12/libpq-ssl.html
		if sslrootcert != "" {
			goto nextCase
		}
		tlsConfig.InsecureSkipVerify = true
		break
	nextCase:
		fallthrough
	case "verify-ca":
		// Don't perform the default certificate verification because it
		// will verify the hostname. Instead, verify the server's
		// certificate chain ourselves in VerifyPeerCertificate and
		// ignore the server name. This emulates libpq's verify-ca
		// behavior.
		//
		// See https://github.com/golang/go/issues/21971#issuecomment-332693931
		// and https://pkg.go.dev/crypto/tls?tab=doc#example-Config-VerifyPeerCertificate
		// for more info.
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(certificates [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(certificates))
			for i, asn1Data := range certificates {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return errors.New("failed to parse certificate from server: " + err.Error())
				}
				certs[i] = cert
			}

			// Leave DNSName empty to skip hostname verification.
			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				Intermediates: x509.NewCertPool(),
			}
			// Skip the first cert because it's the leaf. All others
			// are intermediates.
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, errors.New("sslmode is invalid")
	}

	if sslrootcert != "" {
		caCertPool := x509.NewCertPool()

		caPath := sslrootcert
		caCert, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file: %w", err)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("unable to add CA to cert pool")
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	if sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return nil, fmt.Errorf("unable to read cert: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	switch sslmode {
	case "allow":
		return []*tls.Config{nil, tlsConfig}, nil
	case "prefer":
		return []*tls.Config{tlsConfig, nil}, nil
	case "require", "verify-ca", "verify-full":
		return []*tls.Config{tlsConfig}, nil
	default:
		panic("BUG: bad sslmode should already have been caught")
	}
}

func parsePort(s string) (uint16, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > math.MaxUint16 {
		return 0, errors.New("outside range")
	}
	return uint16(port), nil
}

func makeDefaultDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func makeDefaultResolver() *net.Resolver {
	return net.DefaultResolver
}

func makeDefaultBuildFrontendFunc(minBufferLen int) BuildFrontendFunc {
	return func(r io.Reader, w io.Writer) *pgproto.Frontend {
		cr, err := chunkreader.NewConfig(r, chunkreader.Config{MinBufLen: minBufferLen})
		if err != nil {
			panic(fmt.Sprintf("BUG: chunkreader.NewConfig failed: %v", err))
		}
		frontend := pgproto.NewFrontend(cr, w)

		return frontend
	}
}

func parseConnectTimeoutSetting(s string) (time.Duration, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if timeout < 0 {
		return 0, errors.New("negative timeout")
	}
	return time.Duration(timeout) * time.Second, nil
}

func makeConnectTimeoutDialFunc(timeout time.Duration) DialFunc {
	d := makeDefaultDialer()
	d.Timeout = timeout
	return d.Dial
}

//// ValidateConnectTargetSessionAttrsReadWrite is an ValidateConnectFunc that implements libpq compatible
//// target_session_attrs=read-write.
// TODO validate connect
//func ValidateConnectTargetSessionAttrsReadWrite(ctx context.Context, pgConn *PgConn) error {
//	result := pgConn.ExecParams(ctx, "show transaction_read_only", nil, nil, nil, nil).Read()
//	if result.Err != nil {
//		return result.Err
//	}
//
//	if string(result.Rows[0][0]) == "on" {
//		return errors.New("read only connection")
//	}
//
//	return nil
//}
// TODO fallbacks
//func expandWithIPs(ctx context.Context, lookupFn LookupFunc, fallbacks []*FallbackConfig) ([]*FallbackConfig, error) {
//	var configs []*FallbackConfig
//
//	for _, fb := range fallbacks {
//		// skip resolve for unix sockets
//		if strings.HasPrefix(fb.Host, "/") {
//			configs = append(configs, &FallbackConfig{
//				Host:      fb.Host,
//				Port:      fb.Port,
//				TLSConfig: fb.TLSConfig,
//			})
//
//			continue
//		}
//
//		ips, err := lookupFn(ctx, fb.Host)
//		if err != nil {
//			return nil, err
//		}
//
//		for _, ip := range ips {
//			configs = append(configs, &FallbackConfig{
//				Host:      ip,
//				Port:      fb.Port,
//				TLSConfig: fb.TLSConfig,
//			})
//		}
//	}
//
//	return configs, nil
//}
