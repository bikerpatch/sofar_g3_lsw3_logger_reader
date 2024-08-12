package main

import (
	"errors"
	"os"

	"github.com/kelseyhightower/envconfig"
	"github.com/kubaceg/sofar_g3_lsw3_logger_reader/adapters/export/mosquitto"
	"github.com/kubaceg/sofar_g3_lsw3_logger_reader/adapters/export/otlp"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Inverter struct {
		Port          string   `yaml:"port" envconfig:"INVERTER_PORT"`
		LoggerSerial  uint     `yaml:"loggerSerial" envconfig:"LOGGER_SERIAL_NUMBER"`
		ReadInterval  int      `default:"60" yaml:"readInterval" envconfig:"READ_INTERVAL"`
		LoopLogging   bool     `default:"true" yaml:"loopLogging" envconfig:"LOOP_LOGGING"`
		AttrWhiteList []string `yaml:"attrWhiteList" envconfig:"ATTR_WHITE_LIST"`
		AttrBlackList []string `yaml:"attrBlackList" envconfig:"ATTR_BLACK_LIST"`
	} `yaml:"inverter"`
	Mqtt mosquitto.MqttConfig `yaml:"mqtt"`
	Otlp otlp.Config          `yaml:"otlp"`
}

func (c *Config) validate() error {
	if c.Inverter.Port == "" {
		return errors.New("missing required inverter.port config")
	}

	if c.Inverter.LoggerSerial == 0 {
		return errors.New("missing required inverter.loggerSerial config")
	}

	return nil
}

func NewConfig(configPath string) (*Config, error) {
	config := Config{}

	// Pick up from envs first
	err := envconfig.Process("", &config)
	if err != nil {
		return nil, err
	}

	// Load from file second, overwriting envs, if the file exists
	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		return &config, nil
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	d := yaml.NewDecoder(file)

	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	return &config, nil
}
