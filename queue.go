package ali_mns

import (
	"fmt"
	"os"
	"strings"
)

var (
	DefaultNumOfMessages int32 = 16
)

const (
	PROXY_PREFIX = "MNS_PROXY_"
	GLOBAL_PROXY = "MNS_GLOBAL_PROXY"
)

type AliMNSQueue interface {
	Name() string
	SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error)
	BatchSendMessage(messages ...MessageSendRequest) (resp BatchMessageSendResponse, err error)
	ReceiveMessage(waitseconds ...int64) (MessageReceiveResponse, error)
	BatchReceiveMessage(numOfMessages int32, waitseconds ...int64) (BatchMessageReceiveResponse, error)
	PeekMessage() (MessageReceiveResponse, error)
	BatchPeekMessage(numOfMessages int32) (BatchMessageReceiveResponse, error)
	DeleteMessage(receiptHandle string) (err error)
	BatchDeleteMessage(receiptHandles ...string) (err error)
	ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error)
}

type MNSQueue struct {
	name     string
	client   MNSClient
	stopChan chan bool
	decoder  MNSDecoder
}

func NewMNSQueue(name string, client MNSClient, qps ...int32) AliMNSQueue {
	if name == "" {
		panic("ali_mns: queue name could not be empty")
	}

	queue := new(MNSQueue)
	queue.client = client
	queue.name = name
	queue.stopChan = make(chan bool)
	queue.decoder = NewAliMNSDecoder()

	var attr QueueAttribute
	if _, err := send(client, queue.decoder, GET, nil, nil, "queues/"+name, &attr); err != nil {
		panic(err)
	}

	proxyURL := ""
	queueProxyEnvKey := PROXY_PREFIX + strings.Replace(strings.ToUpper(name), "-", "_", -1)
	if url := os.Getenv(queueProxyEnvKey); url != "" {
		proxyURL = url
	}

	client.SetProxy(proxyURL)

	return queue
}

func (p *MNSQueue) Name() string {
	return p.name
}

func (p *MNSQueue) SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error) {
	_, err = send(p.client, p.decoder, POST, nil, message, fmt.Sprintf("queues/%s/%s", p.name, "messages"), &resp)
	return
}

func (p *MNSQueue) BatchSendMessage(messages ...MessageSendRequest) (resp BatchMessageSendResponse, err error) {
	if messages == nil || len(messages) == 0 {
		return
	}

	batchRequest := BatchMessageSendRequest{}
	for _, message := range messages {
		batchRequest.Messages = append(batchRequest.Messages, message)
	}

	_, err = send(p.client, p.decoder, POST, nil, batchRequest, fmt.Sprintf("queues/%s/%s", p.name, "messages"), &resp)
	return
}

func (p *MNSQueue) ReceiveMessage(waitseconds ...int64) (MessageReceiveResponse, error) {
	resource := fmt.Sprintf("queues/%s/%s", p.name, "messages")
	if waitseconds != nil && len(waitseconds) == 1 && waitseconds[0] >= 0 {
		resource = fmt.Sprintf("queues/%s/%s?waitseconds=%d", p.name, "messages", waitseconds[0])
	}

	resp := MessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) BatchReceiveMessage(numOfMessages int32, waitseconds ...int64) (BatchMessageReceiveResponse, error) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	resource := fmt.Sprintf("queues/%s/%s?numOfMessages=%d", p.name, "messages", numOfMessages)
	if waitseconds != nil && len(waitseconds) == 1 && waitseconds[0] >= 0 {
		resource = fmt.Sprintf("queues/%s/%s?numOfMessages=%d&waitseconds=%d", p.name, "messages", numOfMessages, waitseconds[0])
	}

	resp := BatchMessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) PeekMessage() (MessageReceiveResponse, error) {
	resource := fmt.Sprintf("queues/%s/%s?peekonly=true", p.name, "messages")
	resp := MessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, resource, &resp)
	return resp, err
}

func (p *MNSQueue) BatchPeekMessage(numOfMessages int32) (BatchMessageReceiveResponse, error) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	resp := BatchMessageReceiveResponse{}
	_, err := send(p.client, p.decoder, GET, nil, nil, fmt.Sprintf("queues/%s/%s?numOfMessages=%d&peekonly=true", p.name, "messages", numOfMessages), &resp)
	return resp, err
}

func (p *MNSQueue) DeleteMessage(receiptHandle string) (err error) {
	_, err = send(p.client, p.decoder, DELETE, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s", p.name, "messages", receiptHandle), nil)
	return
}

func (p *MNSQueue) BatchDeleteMessage(receiptHandles ...string) (err error) {
	if receiptHandles == nil || len(receiptHandles) == 0 {
		return
	}

	handlers := ReceiptHandles{}

	for _, handler := range receiptHandles {
		handlers.ReceiptHandles = append(handlers.ReceiptHandles, handler)
	}

	_, err = send(p.client, p.decoder, DELETE, nil, handlers, fmt.Sprintf("queues/%s/%s", p.name, "messages"), nil)
	return
}

func (p *MNSQueue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error) {
	_, err = send(p.client, p.decoder, PUT, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s&VisibilityTimeout=%d", p.name, "messages", receiptHandle, visibilityTimeout), &resp)
	return
}
