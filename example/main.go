package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/jmuyuyang/ali_mns"
)

type appConf struct {
	Url             string `json:"url"`
	Queue           string `json:"queue"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	Delete          bool   `json:"delete"`
}

func main() {
	conf := appConf{}

	if bFile, e := ioutil.ReadFile("app.conf"); e != nil {
		panic(e)
	} else {
		if e := json.Unmarshal(bFile, &conf); e != nil {
			panic(e)
		}
	}

	client := ali_mns.NewAliMNSClient(conf.Url,
		conf.AccessKeyId,
		conf.AccessKeySecret)

	queue := ali_mns.NewMNSQueue(client)
	queue.SetTopic(conf.Queue)
	msg := ali_mns.MessageSendRequest{
		MessageBody: ali_mns.Base64Bytes([]byte("body")),
	}
	fmt.Println(queue.SendMessage(msg))
	fmt.Println(queue.ReceiveMessage())
}
