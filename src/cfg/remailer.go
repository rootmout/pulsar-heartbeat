//
//  Copyright (c) 2020-2021 Datastax, Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
//

package cfg

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"time"
)

const ()

var ()

// Remail consume all messages and referrals by adding the cluster name in the "ping-remailer" field of the message.
// If the message already contains this field or we are the sender, the message is simply discarded.
// To survive the loss of connection, the state of the client and consumer is checked on each iteration and if it was
// closed it is recreated.
func Remail(globalConfig *Configuration, topic TopicCfg) {

	var consumer pulsar.Consumer
	var producer pulsar.Producer
	var client pulsar.Client
	var err error

	tokenSupplier := util.TokenSupplierWithOverride(topic.Token, globalConfig.TokenSupplier())
	client, err = GetPulsarClient(globalConfig, topic.PulsarURL, tokenSupplier)
	SubscriptionName := fmt.Sprintf("%s-%s-remailer", globalConfig.ClusterName, topic.Name)

	if err != nil {
		errMsg := fmt.Sprintf("topic %s, failed to get pulsar client: %v", topic.TopicName, err)
		log.Errorf(errMsg)
		return
	}

	go func() {
		for true {

			// Check if the consumer is alive
			if consumer == nil {
				consumer, err = client.Subscribe(pulsar.ConsumerOptions{
					Topic:                       topic.TopicName,
					SubscriptionName:            SubscriptionName,
					Type:                        pulsar.Exclusive,
					SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
				})

				if err != nil {
					log.Errorf("failed to register consumer for remailer on topic %s. err: %v", topic.TopicName, err)
					time.Sleep(time.Duration(10) * time.Second)
					continue
				}
			}

			// Check if producer is alive.
			if producer == nil {
				producer, err = client.CreateProducer(pulsar.ProducerOptions{
					Topic: topic.TopicName,
				})
				if err != nil {
					log.Errorf(
						"failed to register producer for remailer on topic %s. err: %v",
						topic.TopicName,
						err,
					)
					time.Sleep(time.Duration(10) * time.Second)
					continue
				}
			}

			// Both consumer and producer are ok, we can read for new message.
			message, err := consumer.Receive(context.Background())

			if err != nil {
				log.Errorf("remailer: error reading message on topic %s: %v", topic.TopicName, err)

				// Check if the consumer has been closed because of timeout. Set to nil so in next iteration he
				// will be reopenned.
				pulsarError := err.(*pulsar.Error)
				if pulsarError.Result() == pulsar.ConsumerClosed {
					consumer = nil
				}
				time.Sleep(time.Duration(10) * time.Second)
				continue
			}

			consumer.Ack(message)
			properties := message.Properties()
			log.Debugf(
				"remailer: get message on topic %s. content: %s. ping-from: %s. ping-remailer: %s",
				topic.TopicName,
				message.Payload(),
				properties["ping-from"],
				properties["ping-remailer"],
			)

			// Check if message has to be processed.
			if message == nil {
				log.Errorf("remailre: empty message")
				continue
			}
			if properties["ping-remailer"] != "" {
				log.Infof("remailer: message is already a reply")
				continue
			}

			// Add the local cluster name and return the reponse.
			properties["ping-remailer"] = globalConfig.Name

			response := pulsar.ProducerMessage{
				Payload:    message.Payload(),
				Properties: properties,
				Key:        message.Key(),
			}

			_, err = producer.Send(context.Background(), &response)

			if err != nil {
				log.Errorf("topic %s, failed to remail ping: %v", topic.TopicName, err)

				// Check if the consumer has been closed because of timeout. Set to nil so in next iteration he
				// will be reopenned.
				pulsarError := err.(*pulsar.Error)
				if pulsarError.Result() == pulsar.ProducerClosed {
					producer = nil
				}
				time.Sleep(time.Duration(10) * time.Second)
			}
		}
	}()
}

// RemailerThread Run remailer thread on each topics.
func RemailerThread() {
	globalConfig := GetConfig()
	topics := globalConfig.PulsarTopicConfig
	log.Infof("start remailer on topics : %v", topics)

	for _, topic := range topics {
		Remail(globalConfig, topic)
	}
}
