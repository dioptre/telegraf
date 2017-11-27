package cassandra

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"

	"github.com/gocql/gocql"
)

var (
	// Quote Ident replacer.
	qiReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

// Cassandra struct is the primary data structure for the plugin
type Cassandra struct {
	// URL is only for backwards compatibility
	URL              string
	URLs             []string `toml:"urls"`
	Username         string
	Password         string
	Keyspace         string `toml:"keyspace"`
	UserAgent        string
	RetentionPolicy  string
	WriteConsistency string
	Timeout          internal.Duration
	UDPPayload       int               `toml:"udp_payload"`
	HTTPProxy        string            `toml:"http_proxy"`
	HTTPHeaders      map[string]string `toml:"http_headers"`
	ContentEncoding  string            `toml:"content_encoding"`

	// Path to CA file
	SSLCA string `toml:"ssl_ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl_cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl_key"`
	// Use SSL but skip chain & host verification
	VerifyHost bool `toml:"verify_host"`

	// Precision is only here for legacy support. It will be ignored.
	Precision string

	session *gocql.Session
}

var sampleConfig = `
  ## The full HTTP or UDP URL for your InfluxDB instance.
  ##
  ## Multiple urls can be specified as part of the same cluster,
  ## this means that only ONE of the urls will be written to each interval.
  # urls = ["udp://127.0.0.1:8089"] # UDP endpoint example
  urls = ["http://127.0.0.1:8086"] # required
  ## The target database for metrics (telegraf will create it if not exists).
  database = "telegraf" # required

  ## Name of existing retention policy to write to.  Empty string writes to
  ## the default retention policy.
  retention_policy = ""
  ## Write consistency (clusters only), can be: "any", "one", "quorum", "all"
  write_consistency = "any"

  ## Write timeout (for the InfluxDB client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  # username = "telegraf"
  # password = "metricsmetricsmetricsmetrics"
  ## Set the user agent for HTTP POSTs (can be useful for log differentiation)
  # user_agent = "telegraf"
  ## Set UDP payload size, defaults to InfluxDB UDP Client default (512 bytes)
  # udp_payload = 512

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false

  ## HTTP Proxy Config
  # http_proxy = "http://corporate.proxy:3128"

  ## Optional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## Compress each HTTP request payload using GZIP.
  # content_encoding = "gzip"
`

// Connect initiates the primary connection to the range of provided URLs
func (i *Cassandra) Connect() error {
	var urls []string
	urls = append(urls, i.URLs...)
	cluster := gocql.NewCluster(i.URLs...)
	cluster.Keyspace = i.Keyspace
	cluster.Consistency = gocql.Quorum
	if i.SSLCA != "" {
		sslOpts := &gocql.SslOptions{
			CaPath:                 i.SSLCA,
			EnableHostVerification: i.VerifyHost,
		}
		if i.SSLCert != "" && i.SSLKey != "" {
			sslOpts.CertPath = i.SSLCert
			sslOpts.KeyPath = i.SSLKey
		}
		cluster.SslOpts = sslOpts
	}

	i.session, _ = cluster.CreateSession()

	rand.Seed(time.Now().UnixNano())
	return nil
}

// Close will terminate the session to the backend, returning error if an issue arises
func (i *Cassandra) Close() error {
	if !i.session.Closed() {
		i.session.Close()
	}
	return nil
}

// SampleConfig returns the formatted sample configuration for the plugin
func (i *Cassandra) SampleConfig() string {
	return sampleConfig
}

// Description returns the human-readable function definition of the plugin
func (i *Cassandra) Description() string {
	return "Configuration for cassandra server to send metrics to"
}

// Write will choose a random server in the cluster to write to until a successful write
// occurs, logging each unsuccessful. If all servers fail, return error.
func (i *Cassandra) Write(metrics []telegraf.Metric) error {
	//TODO: performance test against batching
	// This will get set to nil if a successful write occurs
	err := fmt.Errorf("Could not write to any cassandra server in cluster")
	insertBatch := i.session.NewBatch(gocql.UnloggedBatch)
	for _, metric := range metrics {
		var tags = metric.Tags()
		if tags["id"] == "" {
			tags["id"] = gocql.TimeUUID().String()
		}
		if tags["updated"] == "" {
			tags["updated"] = time.Now().Truncate(time.Millisecond).UTC().String()
		}
		serialized, _ := json.Marshal(tags)
		insertBatch.Query(`INSERT INTO logs JSON ?`, string(serialized))
	}
	err = i.session.ExecuteBatch(insertBatch)
	return err
}

func newCassandra() *Cassandra {
	return &Cassandra{
		Timeout: internal.Duration{Duration: time.Second * 5},
	}
}

func init() {
	outputs.Add("cassandra", func() telegraf.Output { return newCassandra() })
}
