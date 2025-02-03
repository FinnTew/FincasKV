package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
	"sync"
	"time"
)

type BaseConfig struct {
	DataDir string
}

type MemIndexConfig struct {
	DataStructure         string
	ShardCount            int
	BTreeDegree           int
	SwissTableInitialSize int
}

type MemCacheConfig struct {
	Enable        bool
	DataStructure string
	Size          int
}

type FileManagerConfig struct {
	MaxSize      int
	MaxOpened    int
	SyncInterval time.Duration
}

type MergeConfig struct {
	Auto     bool
	Interval time.Duration
	MinRatio float64
}

type Config struct {
	Base        BaseConfig
	MemIndex    MemIndexConfig
	MemCache    MemCacheConfig
	FileManager FileManagerConfig
	Merge       MergeConfig
}

var (
	conf     *Config
	confOnce sync.Once
	mu       sync.RWMutex
)

func Get() *Config {
	mu.RLock()
	defer mu.RUnlock()
	return conf
}

func loadConfig(v *viper.Viper) *Config {
	cfg := &Config{}

	cfg.Base.DataDir = v.GetString("base.data_dir")

	cfg.MemIndex.DataStructure = v.GetString("mem_index.data_structure")
	cfg.MemIndex.ShardCount = v.GetInt("mem_index.shard_count")
	cfg.MemIndex.BTreeDegree = v.GetInt("mem_index.btree_degree")
	cfg.MemIndex.SwissTableInitialSize = v.GetInt("mem_index.swiss_table_initial_size")

	cfg.MemCache.Enable = v.GetBool("mem_cache.enable")
	cfg.MemCache.DataStructure = v.GetString("mem_cache.data_structure")
	cfg.MemCache.Size = v.GetInt("mem_cache.size")

	cfg.FileManager.MaxSize = v.GetInt("file_manager.max_size")
	cfg.FileManager.MaxOpened = v.GetInt("file_manager.max_opened")
	cfg.FileManager.SyncInterval = v.GetDuration("file_manager.sync_interval")

	cfg.Merge.Auto = v.GetBool("merge.auto")
	cfg.Merge.Interval = v.GetDuration("merge.interval")
	cfg.Merge.MinRatio = v.GetFloat64("merge.min_ratio")

	return cfg
}

func Init(configPath string) error {
	var initErr error
	confOnce.Do(func() {
		v := viper.New()
		v.SetConfigFile(configPath)

		if err := v.ReadInConfig(); err != nil {
			initErr = err
			log.Printf("read config file failed: %v", err)
			return
		}

		mu.Lock()
		conf = loadConfig(v)
		mu.Unlock()

		v.WatchConfig()
		v.OnConfigChange(func(e fsnotify.Event) {
			log.Printf("config file changed: %s", e.Name)

			newV := viper.New()
			newV.SetConfigFile(configPath)

			if err := newV.ReadInConfig(); err != nil {
				log.Printf("read config file failed: %v", err)
				return
			}

			newConf := loadConfig(newV)

			mu.Lock()
			conf = newConf
			mu.Unlock()

			log.Printf("config file changed and reloaded")
		})
	})

	return initErr
}
