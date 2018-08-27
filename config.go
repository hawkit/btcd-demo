package main

import (
	"os"
	"path/filepath"
	"crypto/rand"
	"encoding/base64"
	"bufio"
	"io"
	"strings"
)

const (
	sampleConfigFilename = "sample-btcd.conf"
)

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