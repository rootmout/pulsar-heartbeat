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
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/topic"
	"github.com/datastax/pulsar-heartbeat/src/util"
)

const (
	latencyBudget = 2400 // in Millisecond integer, will convert to time.Duration in evaluation
	failedLatency = 100 * time.Second
)

var (
	clients         = make(map[string]pulsar.Client)
	partitionTopics = make(map[string]*topic.PartitionTopics)
)

type MsgResult struct {
	InOrderDelivery bool
	Latency         time.Duration
	SentTime        time.Time
}

// PubSubResult give stats for one.
type PubSubResult struct {
	Latency           time.Duration
	InOrderDelivery   bool
	RemainingMessages int
	Success           bool //TODO usefull ?
}

// GetPulsarClient gets the pulsar client object
// Note: the caller has to Close() the client object
func GetPulsarClient(globalConfiguration *Configuration, pulsarURL string, tokenSupplier func() (string, error)) (pulsar.Client, error) {
	client, ok := clients[pulsarURL]
	if !ok {
		clientOpt := pulsar.ClientOptions{
			URL:               pulsarURL,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
		}

		if tokenSupplier != nil {
			clientOpt.Authentication = pulsar.NewAuthenticationTokenFromSupplier(tokenSupplier)
		}

		if strings.HasPrefix(pulsarURL, "pulsar+ssl://") {
			trustStore := globalConfiguration.TrustStore
			if trustStore != "" {
				clientOpt.TLSTrustCertsFilePath = trustStore
			} else {
				log.Warn("missing trustStore while pulsar+ssl tls is enabled")
			}
		}

		pulsarClient, err := pulsar.NewClient(clientOpt)
		if err != nil {
			return nil, err
		}
		clients[pulsarURL] = pulsarClient
		return pulsarClient, nil
	}
	return client, nil
}

// PubSubLatency the latency including successful produce and consume of a message
func PubSubLatency(globalConfiguration *Configuration, clusterName string, topicConfig TopicCfg, tokenSupplier func() (string, error), msgPrefix string, payloads [][]byte, maxPayloadSize int) (map[string]PubSubResult, error) {
	client, err := GetPulsarClient(globalConfiguration, topicConfig.PulsarURL, tokenSupplier)
	if err != nil {
		return nil, err
	}

	// it is important to close client after close of producer/consumer
	// defer client.Close()

	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicConfig.TopicName,
	})

	if err != nil {
		// we guess something could have gone wrong if producer cannot be created
		client.Close()
		delete(clients, topicConfig.PulsarURL)
		return nil, err
	}

	defer producer.Close()

	subscriptionName := "latency-measure-"
	subscriptionName += string(rune(rand.Intn(1000)))

	// use the same input topic if outputTopic does not exist
	// Two topic use case could be for Pulsar function test
	consumerTopic := util.AssignString(topicConfig.OutputTopic, topicConfig.TopicName)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       consumerTopic,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})

	if err != nil {
		defer client.Close() //must defer to allow producer to be closed first
		delete(clients, topicConfig.PulsarURL)
		return nil, err
	}
	defer consumer.Close()

	// notify the main thread with the latency to complete the exit
	completeChan := make(chan map[string]PubSubResult, 1)

	// error report channel
	errorChan := make(chan error, 1)

	// payloadStr := "measure-latency123" + time.Now().Format(time.UnixDate)
	receivedCount := len(payloads) * len(topicConfig.RemoteClusters)

	monitoredClusters := make(map[string]PubSubResult, len(topicConfig.RemoteClusters))

	for _, remoteCluster := range topicConfig.RemoteClusters {
		monitoredClusters[remoteCluster] = PubSubResult{
			InOrderDelivery:   true,
			RemainingMessages: len(payloads),
			Success:           true,
		}
	}

	// Key is payload in string, value is pointer to a MsgResult
	sentPayloads := make(map[string]*time.Time, receivedCount)
	// Use mutex instead of sync.Map in favour of performance and simplicity
	//  and because no need to protect map iteration to calculate results
	mapMutex := &sync.Mutex{}

	receiveTimeout := util.TimeDuration(5+(maxPayloadSize/102400), 10, time.Second)
	go func() {

		for receivedCount > 0 {
			cCtx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
			defer cancel()

			log.Infof("wait to receive on message count %d", receivedCount)
			msg, err := consumer.Receive(cCtx)

			receivedTime := time.Now()

			if err != nil {
				receivedCount = 0 // play safe?
				errorChan <- fmt.Errorf("consumer Receive() error: %v", err)
				break
			} else {
				consumer.Ack(msg)
				log.Infof("received msg with content %s", msg.Payload())
			}

			if msg.Properties()["ping-from"] != clusterName {
				log.Infof("pubsub: ping message initiated by another cluster, discard.")
				continue
			}

			if msg.Properties()["ping-remailer"] == "" {
				log.Infof("pubsub: this message is not a reply, discard.")
				continue
			}

			remoteCluster := msg.Properties()["ping-remailer"]
			clusterResult, ok := monitoredClusters[remoteCluster]

			if !ok {
				log.Warn("pubsub: got response from not monitored cluster")
				continue
			}

			receivedStr := string(msg.Payload())
			currentMsgIndex := GetMessageID(msgPrefix, receivedStr)

			mapMutex.Lock()
			_, ok = sentPayloads[receivedStr]
			mapMutex.Unlock()
			if ok {
				if clusterResult.RemainingMessages == 0 {
					log.Infof("pubsub: got extra message from cluster%s\n", remoteCluster)
				} else {
					if currentMsgIndex != clusterResult.RemainingMessages-1 {
						clusterResult.InOrderDelivery = false
					}
					clusterResult.RemainingMessages--
					receivedCount--
				}

				delay := receivedTime.Sub(*sentPayloads[receivedStr])
				clusterResult.Latency += delay
				if clusterResult.Latency == delay {
					clusterResult.Latency /= 2
				}
				monitoredClusters[msg.Properties()["ping-remailer"]] = clusterResult
			}
			log.Infof("consumer received message index %d from remote-cluster %s, payload size %d\n", currentMsgIndex, remoteCluster, len(receivedStr))
		}

		//successful case all message received
		if receivedCount == 0 {
			completeChan <- monitoredClusters
		}

	}()

	//for _, payload := range payloads {
	for i := len(payloads) - 1; i >= 0; i-- {
		ctx := context.Background()

		// Create a different message to send asynchronously
		asyncMsg := pulsar.ProducerMessage{
			Payload: payloads[i],
			Properties: map[string]string{
				"ping-from": clusterName,
			},
		}

		sentTime := time.Now()
		expectedMsg := expectedMessage(string(payloads[i]), topicConfig.ExpectedMsg) //TODO: what is that ?
		mapMutex.Lock()
		sentPayloads[expectedMsg] = &sentTime
		mapMutex.Unlock()
		// Attempt to send message asynchronously and handle the response
		producer.SendAsync(ctx, &asyncMsg, func(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
			if err != nil {
				errMsg := fmt.Sprintf("fail to instantiate Pulsar client: %v", err)
				log.Infof(errMsg)
				// report error and exit
				errorChan <- errors.New(errMsg)
			}

			log.Infof("successfully published %v", sentTime)
		})
	}

	ticker := time.NewTicker(time.Duration(5*len(payloads)) * time.Second)
	defer ticker.Stop()
	select {
	case receiverLatency := <-completeChan:
		return receiverLatency, nil
	case reportedErr := <-errorChan:
		log.Infof("received error %v", reportedErr)
		return nil, reportedErr
	case <-ticker.C:
		return nil, errors.New("latency measure not received after timeout")
	}
}

// TopicLatencyTestThread tests a message delivery in topic and measure the latency.
func TopicLatencyTestThread() {
	cfg := GetConfig()
	topics := cfg.PulsarTopicConfig
	testBroker := cfg.BrokersConfig.BrokerTestRequired || cfg.K8sConfig.Enabled
	log.Infof("topic configuration %v", topics)

	for _, topic := range topics {
		go func(t TopicCfg) {
			ticker := time.NewTicker(util.TimeDuration(t.IntervalSeconds, 60, time.Second))
			defer ticker.Stop()
			TestTopicLatency(t)
			for {
				select {
				case <-ticker.C:
					if testBroker {
						go TestBrokers(t)
					}
					TestTopicLatency(t)
				}
			}
		}(topic)
	}
}

// TestTopicLatency test generic message delivery in topics and the latency
func TestTopicLatency(topicCfg TopicCfg) {
	// uri is in the form of pulsar+ssl://fqdn:6651
	adminURL, err := url.ParseRequestURI(topicCfg.PulsarURL)
	if err != nil {
		panic(err) //panic because this is a showstopper
	}
	clusterName := adminURL.Hostname()
	tokenSupplier := util.TokenSupplierWithOverride(topicCfg.Token, GetConfig().TokenSupplier())

	if topicCfg.NumberOfPartitions < 2 {
		testTopicLatency(clusterName, tokenSupplier, topicCfg)
	} else {
		testPartitionTopic(clusterName, tokenSupplier, topicCfg)
	}
}

func testTopicLatency(clusterName string, tokenSupplier func() (string, error), topicCfg TopicCfg) {
	globalConfig := GetConfig()
	stdVerdict := util.GetStdBucket(clusterName)
	expectedLatency := util.TimeDuration(topicCfg.LatencyBudgetMs, latencyBudget, time.Millisecond)
	prefix := "messageid"
	payloads, maxPayloadSize := AllMsgPayloads(prefix, topicCfg.PayloadSizes, topicCfg.NumberOfMessages)
	log.Infof("send %d messages to topic %s on cluster %s with latency budget %v, %v, %d",
		len(payloads), topicCfg.TopicName, topicCfg.PulsarURL, expectedLatency, topicCfg.PayloadSizes, topicCfg.NumberOfMessages)
	results, err := PubSubLatency(globalConfig, clusterName, topicCfg, tokenSupplier, prefix, payloads, maxPayloadSize)

	testName := util.AssignString(topicCfg.Name, pubSubSubsystem)

	for remoteClusterName, result := range results {

		log.Infof("cluster %s has message latency %v with remote cluster %s", clusterName, result.Latency, remoteClusterName)
		if err != nil {
			errMsg := fmt.Sprintf("cluster %s, %s latency test Pulsar error: %v", clusterName, testName, err)
			log.Errorf(errMsg)
			if ReportIncident(clusterName, clusterName, "persisted latency test failure", errMsg, &topicCfg.AlertPolicy) && isDowntimeReporting(topicCfg) {
				PromGauge(PubSubDowntimeGaugeOpt(), clusterName, float64(time.Duration(topicCfg.IntervalSeconds)))
			}
		} else if !result.InOrderDelivery {
			errMsg := fmt.Sprintf("cluster %s, %s test Pulsar message received out of order", clusterName, testName)
			log.Errorf(errMsg)
		} else if result.Latency > expectedLatency {
			stdVerdict.Add(float64(result.Latency.Microseconds()))
			errMsg := fmt.Sprintf("cluster %s, %s test message latency %v over the budget %v",
				clusterName, testName, result.Latency, expectedLatency)
			log.Errorf(errMsg)
			if ReportIncident(clusterName, clusterName, "persisted latency test failure", errMsg, &topicCfg.AlertPolicy) && isDowntimeReporting(topicCfg) {
				PromGauge(PubSubDowntimeGaugeOpt(), clusterName, float64(time.Duration(topicCfg.IntervalSeconds)))
			}
		} else if stddev, mean, within6Sigma := stdVerdict.Push(float64(result.Latency.Microseconds())); !within6Sigma && stddev > 0 && mean > 0 {
			errMsg := fmt.Sprintf("cluster %s, %s test message latency %v μs over six standard deviation %v μs and mean is %v μs",
				clusterName, testName, result.Latency.Microseconds(), stddev, mean)
			// 5 ms = 5,000 μs
			if mean > 5000 {
				errMsg = fmt.Sprintf("cluster %s, %s test message latency %v over six standard deviation %v ms and mean is %v ms",
					clusterName, testName, result.Latency, float64(stddev/1000.0), float64(mean/1000.0))
			}
			log.Errorf(errMsg)
		} else {
			log.Infof("succeeded to sent %d messages to topic %s on %s test cluster %s",
				len(payloads), topicCfg.TopicName, testName, topicCfg.PulsarURL)
			ClearIncident(clusterName)
			if isDowntimeReporting(topicCfg) {
				PromGauge(PubSubDowntimeGaugeOpt(), clusterName, 0) // report gauge no downtime
			}
		}
		if result.Latency < failedLatency {
			PromLatencySum(GetGaugeType(topicCfg.Name), clusterName, remoteClusterName, result.Latency)
		}

	}
}

func isDowntimeReporting(cfg TopicCfg) bool {
	return !cfg.DowntimeTrackerDisabled && cfg.NumberOfPartitions == 1 && cfg.ClusterName != ""
}

func expectedMessage(payload, expected string) string {
	if strings.HasPrefix(expected, "$") {
		return fmt.Sprintf("%s%s", payload, expected[1:len(expected)])
	}
	return payload
}

func testPartitionTopic(clusterName string, tokenSupplier func() (string, error), cfg TopicCfg) {
	trustStore := util.AssignString(cfg.TrustStore, GetConfig().TrustStore)
	testName := "partition-topics-test"
	component := clusterName + "-" + testName
	pt, err := getPartition(cfg, tokenSupplier, trustStore)
	if err != nil {
		errMsg := fmt.Sprintf("%s failed to create PartitionTopic test object, error: %v", component, err)
		ReportIncident(component, component, "persisted failure to create partition topic test client", errMsg, &cfg.AlertPolicy)
		return
	}
	pulsarClient, err := GetPulsarClient(GetConfig(), cfg.PulsarURL, tokenSupplier)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s failed create Pulsar Client with error: %v", component, testName, err)
		log.Errorf(errMsg)
		ReportIncident(component, component, "partition topic test failure", errMsg, &cfg.AlertPolicy)
		return
	}

	latency, err := pt.TestPartitionTopic(pulsarClient)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s partition topic test failed with Pulsar error: %v", component, testName, err)
		log.Errorf(errMsg)
		ReportIncident(component, component, "partition topic test failure", errMsg, &cfg.AlertPolicy)
		return
	}
	expectedLatency := util.TimeDuration(cfg.LatencyBudgetMs, latencyBudget, time.Millisecond)
	if latency > expectedLatency || latency == 0 {
		errMsg := fmt.Sprintf("cluster %s, partition topic test message latency %v over the budget %v",
			component, latency, expectedLatency)
		log.Errorf(errMsg)
		ReportIncident(component, component, "partition topic test has over budget latency", errMsg, &cfg.AlertPolicy)
	} else {
		log.Infof("%d partition topics test successfully passed with latency %v", pt.NumberOfPartitions, latency)
		ClearIncident(component)
	}
}

func getPartition(cfg TopicCfg, tokenSupplier func() (string, error), trustStore string) (*topic.PartitionTopics, error) {
	pt, ok := partitionTopics[cfg.TopicName]
	if !ok {
		var err error
		pt, err = topic.NewPartitionTopic(cfg.PulsarURL, tokenSupplier, trustStore, cfg.TopicName, cfg.AdminURL, cfg.NumberOfPartitions)
		if err != nil {
			return nil, err
		}
		partitionTopics[cfg.TopicName] = pt
	}

	return pt, pt.VerifyPartitionTopic()
}
