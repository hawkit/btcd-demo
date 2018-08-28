package main

import (
	"os"
	"path/filepath"
	"crypto/rand"
	"encoding/base64"
	"bufio"
	"io"
	"strings"
	"time"
	"net"
	"btcd-demo/chaincfg"

	"github.com/hawkit/btcutil-demo"
)

const (
	defaultConfigFilename = "btcd.conf"
	dafaultDataDirname = "data"
	defaultLogLevel = "info"
	defaultLogDirname = "logs"
	defaultLogFilename = "btcd.log"
	defaultMaxPeers = 125
	defaultBanDuration = time.Hour * 24
	defaultBanThreshold = 100
	defaultConnectTimeout = time.Second * 30
	defaultMaxRPCClients = 10
	defaultMaxRPCWebsockets = 25
	defaultMaxRPCConcurrentReqs = 20
	defaultDbType = "ffldb"
	defaultFreeTxRelayLimit = 15.0
	defaultTrickleInterval =  10 * time.Second // todo peer.DefaultTrickleInterval
	defaultBlockMinSize = 0
	defaultBlockMaxSize = 750000
	defaultBlockMinWeight = 0
	defaultBlockMaxWeight = 3000000
	blockMaxSizeMin = 1000
	blockMaxSizeMax = 1000 //todo blockchain.MaxBlockBaseSize - 1000
	blockMaxWeightMin = 4000
	blockMaxWeightMax = 4000 //todo blockchain.MaxBlockWeight - 4000
	defaultGenerate = false
	defaultMaxOrphanTransactions = 100
	defaultMaxOrphanTxSize = 100000
	defaultSigCacheMaxSize = 100000
	sampleConfigFilename = "smaple-btcd.conf"
	defaultTxIndex = false
	defaultAddrIndex = false
)

var (

	defaultHomeDir = btcutil.AppDataDir("btcd", false)
	defaultConfigFile = filepath.Join(defaultHomeDir, sampleConfigFilename)
	defaultDataDir = filepath.Join(defaultHomeDir, dafaultDataDirname)
	defaultLogDir = filepath.Join(defaultHomeDir, defaultLogFilename)
	defaultRPCKeyFile = filepath.Join(defaultHomeDir, "rpc.key")
	defaultRPCCertFile = filepath.Join(defaultHomeDir, "rpc.cert")
)
// config defines the configuration options for btcd
// see loadConfig for details on the configuration load process.
type config struct {

	ShowVersion bool `short:"V"" long:"version" description:"Display version information and exit"`
	ConfigFile string `short:"C"" long:"configfile"" description:"Path to configuration file"`
	DataDir string 	`short:"b"" long:"datadir"" description:"Directory to store data"`
	LogDir string `long:"logdir" description:"Directory to log output."`
	AddPeers []string `short:"a" long:"addpeer" description:"Add a peer to connect with at startup"`
	ConnectPeers []string `long:"connect" description:"Connect only to the specified peers at startup"`
	DisableListen bool `long:"onlisten" description:"Disable listening for incoming connections -- NOTE: listening is automatically disable if the --connect or --proxy are used without also specifying listen interfaces via -- listen"`
	Listeners []string `long:"listen" description："Add an interface/port to listen for connections(default all interface port: 8333, testnet: 18333)"`
	MaxPeers int `long:"maxpeers" description:"Max number of inbound and outbound peers"`
	DisableBanning bool `long:"nobanning" description:Disable banning of misbehaving peers`
	BanDuration time.Duration `long:"banduration" description:"How long to ban misbehaving peers. Valid time units are {s, m, h}. Minimum 1 second"`
	BanThreshold uint32 `long:"banthreshold" description:"Maximum allowed ban score before disconnecting and banning misbehaving peers"`
	Whitelists	[]string `long"whitelist" description:"Add an IP network or IP that will not be banned. (eg. 192.168.1.0/24 or ::1)"`
	RPCUser string `short:"u" long:"rpcuser" description:"Username for RPC connections"`
	RPCPass string `short:"p" long:"rpcpass" default-mask:"-" description:"Password for RPC connections"`
	RPCLimitUser string `long:"rpclimituser" description:"Username for limited RPC connections"`
	RPCLimitPass string `long:"rpclimitpass" default-mask:"-" description:"Password for limited RPC connections"`
	RPCListeners []string `long:"rpclisten" description:"Add an interface/port to listen for RPC connections(default port: 8334, testnet: 18334)"`
	RPCCert	string `long:"rpccert" description:"File containing the certification file"`
	RPCKey string `long:"rpckey" description:"File containing the certificate key"`
	RPCMaxClients int 	`long:"rpcmaxclients" description:"Max number of RPC clients for standard connections"`
	RPCMaxWebsockets int `long:"rpcmaxwebsockets" description:"Max number of RPC websocket connections"`
	RPCMaxConcurrentReqs int `long:"rpcmaxconcurrentreqs" description:"Max number of concurrent RPC requests that may be processed concurrently"`
	RPCQuirks bool 	`long:"rpcquirks" description:"Mirrow some JSON-RPC quirks of Bitcoin Core -- NOTE: Discouraged unless interoperability issues need to be worked around"`
	DisableRPC bool `long:"norpc" description:"Disable built-in RPC server -- NOTE: The RPC server is disabled by default if no rpcuser/rpcpass or rpclimituser/rpclimitpass is specified"`
	DisableTLS bool `long:"notls" description:"Disable TLS for the RPC server -- NOTE: this is only allowed if the RPC server is bound to localhost"`
	DisableDNSSeed bool `long:"nodnsssed" description:"Disable DNS seeding for peers"`
	ExternalIPs []string `long:"externalip" description:"Add an ip to the list of local addresses we claim to listen on to peers"`
	Proxy 	string `long:"proxy" description:"Connect via SOCKET5 proxy (eg. 127.0.0.1:9050)"`
	ProxyUser string `long:"proxyuser" description:"Username for proxy server"`
	ProxyPass string `long:"proxypass" default-mask:"-" description:"Password for proxy server"`
	OnionProxy string `long:"onion" description:"Connect to tor hide services via SOCK5 proxy (eg. 127.0.0.1:9050)"`
	OnionProxyUser string `long:"onionuser" description:"Username for onion proxy server"`
	OnionProxyPass string `long:"onionpass" default-mask:"-" description:"Password for onion proxy server"`
	NoOnion bool 	`long:"noonion" description:"Disable connecting to tor hide services"`
	TorIsolation bool 	`long:"torisolation" description:"Enable Tor stream isolation by randomizing user credentials for each connection."`
	TestNet3 bool `long:"testnet" description:"Use the test network"`
	RegressionTest bool `bool:"regtest" description:"Use the regression test network"`
	SimNet bool `long:"simnet" description:"Use the simulation test network"`
	AddCheckpoints []string `long:"addcheckpoint" description:"Add a custom checkpoint. Format:'<height>:<hash>'"`
	DisableCheckpoints bool `long:"nocheckpoint" description:"Disable built-in checkpoints. Don't do this unless you know what you're doing.'"`
	DbType string `long:"dbtype" description:"Database backend to use for the Block Chain."`
	Profile string `long:"profile" description:"Enable HTTP profiling on given port -- NOTE port must between 1024 and 65536"`
	CPUProfile string `long:"cpuprofile" description:"Write CPU profile to the specified file"`
	DebugLevel string `short:"d" long:"debuglevel" description:"Logging level for all subsystems {trace, debug, info, warn, error, critical} -- You may also specify <subsytem>=<level>, ... to se the log level for individual subsystems -- Use show to list available subsystems"`
	Upnp bool `long:"upnp" description:"Use UPnP to map our listening port outside of NAT"`
	MinRelayTxFee float64 `long:"minrelaytxfee" description:"The minimum transaction fee in BTC/kB to be considered a non-zero fee"`
	FreeTxRelayLimit float64 `long:"limitfreerelay" description:"Limit relay of transactions with no transaction fee to the given amount in thousands of bytes per minute"`
	NoRelayPriority bool `long:"norelaypriority" description:"Do not require free or low-fee transaction fee to the given amount in thousands of bytes per minute"`
	TrickleInterval time.Duration `long:"trickleinterval" description:"Minimum time between attempts to send new inventory to a new connected peer"`
	MaxOrphanTxs int `long:"maxorphantxs" description:"Max number of orphan transactions to keep in memory"`
	Generate bool `long:"generate" description:"Generate(mine) bitcoins using the CPU"`
	MiningAddrs []string `long:"miningaddr" description:"Add the specified payment address to the list of address to use for generated blocks -- At least one address is required if the generate option is set"`
	BlockMinSize uint32 `long:"blockminsize" description:"Minimum block size in bytes to be used when creating a block"`
	BlockMaxSize uint32 `long:"blockmaxsize" description:"Maximum block size in bytes to be used when creating a block"`
	BlockMinWeight uint32 `long:"blockminweight" description:"Minimum block weight to be used when creating a block"`
	BlockMaxWeight uint32 `long:"blockmaxweight" description:"Maximum block weight to be used when creating a block"`
	BlockPrioritySize uint32 `long:"blockprioritySize" description:"Size in bytes for high-priority/low-free transactions when creating a block"`
	UserAgentComments []string `long:"uacomment" description:"Comment to add to the user agent -- See BIP 14 for more information"`
	NoPeerBloomFilters bool `long:"nopeerbloomfilters" description:"Disable bloom filtering support"`
	NoCFilters bool `long:"nocfilters" description:"Disable committed filtering(CF) support"`
	DropCfIndex bool `long:"dropcfindex" description:"Deletes the index used for committed filtering (CF) support from the database on start up and then exist"`
	SigCacheMaxSize uint `long:"sigcachemaxsize" description:"The maximum number of entries in the signature verification cache"`
	BlocksOnly bool `long:"blocksonly" description:"Do not accept transactions from remote peers."`
	TxIndex bool `long:"txindex" description:"Maintain a full hash-based transaction index which makes all transactions available via the getrawtransaction RPC"`
	DropTxIndex bool `long:"droptxindex" description:"Deletes the hash-based transaction index form the database on start up and then exits. "`
	AddrIndex bool `long:"addrindex" description:"Maintain a full address-based transaction index which makes the searchrawtransactions RPC available"`
	DropAddrIndex bool `long:"dropaddrindex" description:"Deletes the address-based transaction index from the database on start up and then exists."`
	RelayNonStd bool `long:"relaynonstd" description:"Relay non-standard transactions regardless of the default settings for the active network."`
	RejectNonStd bool `long:"rejectnonstd" description:"Reject non-standard transactions regardless of the default setting for the active network."`

	lookup func(string) ([]net.IP, error)
	oniondial func(string, string, time.Duration) (net.Conn, error)
	dial func(string, string, time.Duration) (net.Conn, error)
	addCheckpoints []chaincfg.Checkpoint
	miningAddrs []btcutil.Address
	minRelayTxFee btcutil.Amount
	whitelists []*net.IPNet

}

// serviceOptions defines the configuration options for the daemon as a service on
// Windows.
type serviceOptions struct {
	ServiceCommand string `short:"s" long:"service" description:"Service command {install, remove, start, stop}"`
}

// newConfigParser returns a new command line flags parser
//func newConfigParser(cfg *config, so *serviceOptions, options flags.Options) *flags.Parser {
//
//}

// loadConfig 通过配置文件，命令行选项来初始化配置
//
// 配置处理按下面的流程进行：
// 1 ) 启动时，使用默认配置
// 2 ) 预解释命令行，检查是否有另外指定的配置文件
// 3 ) 加载配置文件，并覆盖默认设置
// 4 ) 解释命令行选项，覆盖或添加新指定的配置
//


func loadConfig()(*config, []string, error)  {
	// Default config.
	cfg := config{
		ConfigFile: defaultConfigFile,
		DebugLevel: defaultLogLevel,
		MaxPeers: defaultMaxPeers,
		BanDuration:defaultBanDuration,
		BanThreshold:defaultBanThreshold,
		RPCMaxClients:defaultMaxRPCClients,
		RPCMaxWebsockets:defaultMaxRPCWebsockets,
		RPCMaxConcurrentReqs:defaultMaxRPCConcurrentReqs,
		DataDir:defaultDataDir,
		LogDir:defaultLogDir,
		DbType:defaultDbType,
		RPCKey:defaultRPCKeyFile,
		RPCCert:defaultRPCCertFile,
		MinRelayTxFee: btcutil.Amount(1000).ToBTC(), //todo   mempool.DefaultMinRelayTxFee.ToBTC(),
		FreeTxRelayLimit:defaultFreeTxRelayLimit,
		TrickleInterval: defaultTrickleInterval,
		BlockMinSize:defaultBlockMinSize,
		BlockMaxSize:defaultBlockMaxSize,
		BlockMinWeight:defaultBlockMinWeight,
		BlockMaxWeight:defaultBlockMaxWeight,
		BlockPrioritySize:50000, //todo  mempool.DefaultBlockPrioritySize,
		MaxOrphanTxs:defaultMaxOrphanTxSize,
		SigCacheMaxSize:defaultSigCacheMaxSize,
		Generate:defaultGenerate,
		TxIndex:defaultTxIndex,
		AddrIndex:defaultAddrIndex,
	}

	// Service options which are only added on windows
	//serviceOpts := serviceOptions{}

	// Pre-parse the command line options to see if an alternative config
	// file or the version flag aws specified. Any errors aside from the
	// help message error can be ignored here since they will be caught by
	// final parse below
	//preCfg := cfg
	//preParser := newConfigParser(&preCfg, &serviceOpts, flags.HelpFlag)

	return &cfg, nil, nil
}

// createDefaultConfig copies the file sample-btcd.conf to the given destination path,
// and populates it with some randomly generated RPC username and password.
func createDefaultConfigFile(destinationPath string) error  {
	// Create the destination directory if it does not exists
	err := os.MkdirAll(filepath.Dir(destinationPath), 0700)
	if err != nil {
		return err
	}

	// We assume sample config file path is same as binary
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return err
	}
	sampleConfigFile := filepath.Join(path, sampleConfigFilename)

	// We generate a random user and password
	randomBytes := make([]byte, 20)
	_, err = rand.Read(randomBytes)
	if err != nil {
		return err
	}
	generatedRPCUser := base64.StdEncoding.EncodeToString(randomBytes)

	_, err = rand.Read(randomBytes)
	if err != nil {
		return err
	}
	generatedPass := base64.StdEncoding.EncodeToString(randomBytes)

	src, err := os.Open(sampleConfigFile)
	if err!= nil {
		return err
	}
	defer src.Close()

	dest, err := os.OpenFile(destinationPath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	// We copy every line from the sample config file to the destination,
	// only replacing the two lines for rpcuser and rpcpass
	reader := bufio.NewReader(src)
	for err != io.EOF {
		var line string
		line, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}

		if strings.Contains(line, "rpcuser=") {
			line = "rpcuser=" + generatedRPCUser + "\n"
		} else if strings.Contains(line, "rpcpass=") {
			line = "rpcpass=" + generatedPass + "\n"
		}

		if _, err := dest.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}