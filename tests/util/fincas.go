package util

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"
)

func getRootPath() string {
	var rootPath string
	_, filename, _, ok := runtime.Caller(1)
	if ok {
		rootPath = path.Dir(filename)
		rootPath = path.Dir(rootPath)
		rootPath = path.Dir(rootPath)
	}
	return rootPath
}

func getBinPath() string {
	rootPath := getRootPath()
	var binPath string
	if len(rootPath) != 0 {
		binPath = path.Join(rootPath, "bin", "fincas")
	}
	return binPath
}

func checkCondition(c *redis.Client) bool {
	ctx := context.TODO()
	_, err := c.Ping(ctx).Result()
	return err == nil
}

type Srv struct {
	cmd     *exec.Cmd
	addr    *net.TCPAddr
	delete  bool
	dataDir string
}

func (s *Srv) Addr() string {
	return s.addr.String()
}

func (s *Srv) NewClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         s.Addr(),
		DB:           0,
		DialTimeout:  10 * time.Minute,
		ReadTimeout:  10 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		MaxRetries:   -1,
		PoolSize:     30,
		PoolTimeout:  10 * time.Minute,
	})
}

func (s *Srv) Close() error {
	err := s.cmd.Process.Kill()

	done := make(chan error, 1)
	go func() {
		done <- s.cmd.Wait()
	}()

	timeout := time.After(30 * time.Second)

	select {
	case <-timeout:
		log.Println("exec.cmd.Wait timeout")
		if err = s.cmd.Process.Kill(); err != nil {
			log.Println("exec.cmd.Process.Kill timeout")
			return err
		}
	case err = <-done:
		break
	}

	if s.delete {
		log.Println("clean env")
		err := os.RemoveAll(s.dataDir)
		if err != nil {
			log.Println("remove data dir failed")
			return err
		}
		log.Println("remove data dir success")
	}

	log.Println("close server success")

	return nil
}

func StartServer(port int, del bool) *Srv {
	var (
		p = 8911
	)

	b := getBinPath()
	c := exec.Command(b)
	t := time.Now().UnixMilli()

	if port != 0 {
		p = port
	}

	logDir := path.Join(getRootPath(), "log")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		panic(err)
	}
	logName := fmt.Sprintf("test_%d_%d.log", t, p)
	outFile, err := os.Create(path.Join(logDir, logName))
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	c.Stdout = outFile
	c.Stderr = outFile
	log.SetOutput(outFile)

	dataDir := path.Join(getRootPath(), fmt.Sprintf("fincas_data_%d_%d", t, p))

	c.Args = append(c.Args, "--dir", dataDir, "--port", fmt.Sprintf("%d", p))

	err = c.Start()
	if err != nil {
		log.Println("fincas start failed")
		return nil
	}

	addr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: p,
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: addr.String(),
	})

	count := 0
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		count++
		if checkCondition(rdb) {
			log.Println("fincas start success")
			break
		} else if count == 12 {
			log.Println("fincas start failed")
			return nil
		} else {
			log.Printf("fincas start failed %s, retry %d\n", addr.String(), count)
		}
	}

	log.Println("fincas start success")

	return &Srv{
		cmd:     c,
		addr:    addr,
		delete:  del,
		dataDir: dataDir,
	}
}
