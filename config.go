package main


type config struct {

	Profile string
	CPUProfile string
	DropAddrIndex bool
	DropTxIndex bool
	DropCfIndex bool
	Listeners []string
}

// loadConfig 通过配置文件，命令行选项来初始化配置
//
// 配置处理按下面的流程进行：
// 1 ) 启动时，使用默认配置
// 2 ) 预解释命令行，检查是否有另外指定的配置文件
// 3 ) 加载配置文件，并覆盖默认设置
// 4 ) 解释命令行选项，覆盖或添加新指定的配置
//


func loadConfig()(*config, []string, error)  {
	return &config{}, nil, nil
}