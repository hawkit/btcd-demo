package main

import (
	"runtime/debug"
	"runtime"
	"fmt"
	"os"
	"net"
	"btcd-demo/database"
	"btcd-demo/limits"
)

var (
	cfg *config
)



func winServiceMain()(bool, error) {
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
	defer func(){
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

func loadBlockDB()(database.DB, error)  {
	return &database.SimpleDB{}, nil
}

func main()  {

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