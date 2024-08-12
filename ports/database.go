package ports

import mqtt "github.com/eclipse/paho.mqtt.golang"

type Database interface {
	InsertDiscoveryRecord(discovery string, prefix string, loggerSerial uint, fields []DiscoveryField) error
	InsertRecord(topic string, measurement map[string]interface{}) error
}

type DatabaseWithListener interface {
	Database
	Subscribe(topic string, callback mqtt.MessageHandler)
}
