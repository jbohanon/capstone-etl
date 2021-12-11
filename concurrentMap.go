package main

import (
	"github.com/pkg/errors"
)

type (
	// ConcurrentMap is a wrapper to implement concurrent read/write safe map[string]*interface{}
	ConcurrentMap interface {
		Get(key string) interface{}
		Put(key string, val interface{}) error
		Delete(key string) error
		Keys() []string
		readWriteHandler()
	}
	concurrentMap struct {
		m map[string]interface{}
		readChan chan concurrentMapTransport
		writeChan chan concurrentMapTransport
		deleteChan chan concurrentMapTransport
		keysChan chan concurrentMapTransport
	}

	concurrentMapTransport struct {
		retChan chan interface{}
		str            string
		val interface{}
	}
)

func NewConcurrentMap() ConcurrentMap {
	m := concurrentMap{
		m:         make(map[string]interface{}),
		readChan:  make(chan concurrentMapTransport),
		writeChan: make(chan concurrentMapTransport),
		deleteChan: make(chan concurrentMapTransport),
		keysChan : make(chan concurrentMapTransport),
	}

	go m.readWriteHandler()
	return &m
}

func (m *concurrentMap) Get(key string) interface{} {
	retChan := make(chan interface{})
	t := concurrentMapTransport{
		retChan: retChan,
		str: key,
	}
	m.readChan <- t
	for msg := range retChan {
		return msg
	}
	return nil
}

func (m *concurrentMap) Put(key string, val interface{}) error {
	retChan := make(chan interface{})
	t := concurrentMapTransport{
		retChan:        retChan,
		str:            key,
		val: val,
	}
	m.writeChan <- t
	for msg := range retChan {
		if msg != val {
			return errors.New("error occurred writing the value")
		}
		return nil
	}
	return nil
}

func (m *concurrentMap) Delete(key string) error {
	retChan := make(chan interface{})
	t := concurrentMapTransport{
		retChan:        retChan,
		str:            key,
	}
	m.deleteChan <- t
	for msg := range retChan {
		if msg != nil {
			return errors.New("error occurred deleting the value")
		}
		return nil
	}
	return nil
}

func (m *concurrentMap) Keys() []string {
	retChan := make(chan interface{})
	t := concurrentMapTransport{
		retChan: retChan,
	}
	m.keysChan <- t
	for msg := range retChan {
		return msg.([]string)
	}
	return nil

}

func (m *concurrentMap) readWriteHandler() {
	for {
		select {
		case msg := <-m.readChan:
			msg.retChan <- m.m[msg.str]
		case msg := <-m.writeChan:
			m.m[msg.str] = msg.val
			msg.retChan <- msg.val
		case msg := <-m.deleteChan:
			delete(m.m, msg.str)
			msg.retChan <- nil
		case msg := <-m.keysChan:
			var retSl []string
			for k := range m.m {
				retSl = append(retSl, k)
			}
			msg.retChan <- retSl
		}
	}
}