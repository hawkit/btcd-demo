package main

import (
	"btcd-demo/database"
	"btcd-demo/limits"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
)

const (
	// blockDbNamePrefix is the prefix for the block database name.  The
	// database type is appended to this value to form the full block
	// database name.
	blockDbNamePrefix = "blocks"
)

var (
	cfg *config
)

func winServiceMain() (bool, error) {
	fmt.Println("btcd service runing...")
	return true, nil
}

func btcdMain(serverChan chan<- *server) error {

	// 加载配置，解析命令行选项。
	tcfg, _, err := loadConfig()
	if err != nil {
		return err
	}
	cfg = tcfg

	interrupt := interruptListener()
	defer btcdLog.Info("Shutdown complete")

	// 打印版本信息
	btcdLog.Infof("Version %s", version())

	// 开启http服务器性能监测
	if cfg.Profile != "" {
		go func() {
			listenAddr := net.JoinHostPort("", cfg.Profile)
			btcdLog.Infof("Profile server listening on %s", listenAddr)
		}()
	}
	// 开启cpu性能监测
	if cfg.CPUProfile != "" {
		btcdLog.Info("CPU Profile start")
	}

	// 升级老版本数据库相关文件路径
	if err := doUpgrades(); err != nil {
		btcdLog.Errorf("%v", err)
		return err
	}

	// 是否有中断信号
	if interruptRequested(interrupt) {
		return nil
	}

	// 加载区块数据库
	db, err := loadBlockDB()
	if err != nil {
		btcdLog.Errorf("%v", err)
		return err
	}
	defer func() {
		btcdLog.Info("Gracefully shutting down the database...")
		db.Close()
	}()

	// 是否有中断信号
	if interruptRequested(interrupt) {
		return nil
	}

	// 删除地址索引，并退出程序
	//
	// 注意： 在这里删除顺序很重要，因为删除交易索引同时也会删除地址索引。
	// 因为交易索引会依赖地址索引
	if cfg.DropAddrIndex {
		btcdLog.Info("Drop Address index done")
	}
	// 删除交易索引，并退出程序
	if cfg.DropTxIndex {
		btcdLog.Info("Drop transaction index done")
	}
	// 删除交易索引，并退出程序
	if cfg.DropCfIndex {
		btcdLog.Info("Drop cf(committed filter) index done")
	}

	// 创建服务器，并启动
	server, err := newServer(cfg.Listeners, db, activeNetParams.Params, interrupt)
	if err != nil {
		btcdLog.Errorf("Failed to start server on %v: %v", cfg.Listeners, err)
		return err
	}
	defer func() {
		btcdLog.Infof("Gracefully shutting down the server...")
		server.Stop()
		server.WaitForShutdown()
	}()

	server.Start()
	if serverChan != nil {
		serverChan <- server
	}

	//挂起主线程，等待中断信号
	<-interrupt
	return nil
}

// removeRegressionDB removes the existing regression test database if running
// in regression test mode and it already exists
func removeRegressionDB(dbPath string) error {
	if !cfg.RegressionTest {
		return nil
	}

	// Remove the old regression test database if it already exists.
	fi, err := os.Stat(dbPath)
	if err == nil {
		btcdLog.Infof("Removing regression test database from '%s'", dbPath)
		if fi.IsDir() {
			err := os.RemoveAll(dbPath)
			if err != nil {
				return err
			}
		} else {
			err := os.Remove(dbPath)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

// blockDbPath returns the path to the block database given a database type
func blockDbPath(dbType string) string {
	// The database name is base on the database type.
	dbName := blockDbNamePrefix + "_" + dbType
	if dbType == "sqlite" {
		dbName = dbName + ".db"
	}
	dbPath := filepath.Join(cfg.DataDir, dbName)
	return dbPath
}

// warnMultipleDBs shows a warning if multiple block database types are detected.
// This is not situation most user want. It is handy for development hower
// to support multiple side-by-side databases.
func warnMultipleDBs() {
	// This is intentionally not using the known db types which depend
	// on the database types compiled into the binary since we want to
	// detect legacy db types as well.
	dbTypes := []string{"ffldb", "leveldb", "sqlite"}
	duplicateDbPaths := make([]string, 0, len(dbTypes)-1)
	for _, dbType := range dbTypes {
		if dbType == cfg.DbType {
			continue
		}

		// Store db path as a duplicate db if it exists
		dbPath := blockDbPath(dbType)
		if fileExists(dbPath) {
			duplicateDbPaths = append(duplicateDbPaths, dbPath)
		}
	}

	// Warn if there are extra databases.
	if len(duplicateDbPaths) > 0 {
		selectedDbPath := blockDbPath(cfg.DbType)
		btcdLog.Warnf("WARNING: There are multiple block chain databases "+
			"using different database types. \n You probably don't "+
			"want to waste disk space by having more than one.\n"+
			"Your current database is located at [%v].\n "+
			"The additional database is located at %v", selectedDbPath, duplicateDbPaths)
	}
}

// loadBlockDB loads (or creates when needed) the block database taking into
// account the selected database backend and returns a handle to it. It also
// contains additional logic such warning the user if there are multiple
// databases which consume space on the file system and ensuring the regestion
// test database is clean when in regression test mode.
func loadBlockDB() (database.DB, error) {
	// The memdb backend does not have a file path associated with it, so
	// handle it uniquely. We also don't want to worry about the multiple
	// database type warnings when running with the memory database.
	if cfg.DbType == "memdb" {
		btcdLog.Infof("Creating block database in memory.")
		db, err := database.Create(cfg.DbType)
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	warnMultipleDBs()

	// The database name is based on the database type.
	dbPath := blockDbPath(cfg.DbType)

	// The regression test is special in that it needs a clean database for
	// each run, so remove it now if it already exists.
	removeRegressionDB(dbPath)

	btcdLog.Infof("Loading block database from '%s'", dbPath)

	db, err := database.Open(cfg.DbType, dbPath, activeNetParams.Net)
	if err != nil {
		// Return the error if it's not because the database doesn't
		// exist.
		if dbErr, ok := err.(database.Error); !ok ||
			dbErr.ErrorCode != database.ErrDbDoesNotExist {
			return nil, err
		}

		// Create the db if it dose not exist.
		err = os.MkdirAll(cfg.DataDir, 0700)
		if err != nil {
			return nil, err
		}
		db, err = database.Create(cfg.DbType, dbPath, activeNetParams.Net)
		if err != nil {
			return nil, err
		}
	}

	btcdLog.Infof("Block database loaded")
	return db, nil
}

func main() {

	//设置cpu最大使用数量，这里全部使用
	runtime.GOMAXPROCS(runtime.NumCPU())

	//设置触发GC垃圾收集器阀值： 10% ， 即 最新分布的内存大小/上次GC之后活动内存大小*100
	debug.SetGCPercent(10)

	//设置进程能打开的最大文件资源,即file descriptor数量。做linux系统中，所有资源(pipe, socket, file等等)都以文件形式出现。
	if err := limits.SetLimits(); err != nil {
		fmt.Printf("Failed to set limits: %v \n", err)
		os.Exit(1)
	}

	if runtime.GOOS == "windows" {
		isService, err := winServiceMain()
		if err != nil {
			fmt.Printf("Failed to start windows service: %v \n", err)
		}
		if isService {
			os.Exit(0)
		}
	}

	if err := btcdMain(nil); err != nil {
		fmt.Printf("Failed to start btcd server: %v", err)
		os.Exit(1)
	}
}
