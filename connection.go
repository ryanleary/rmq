package rmq

import (
	"fmt"
	"log"
	"strings"
	"time"

	"gopkg.in/redis.v5"

	"github.com/adjust/uniuri"
)

const heartbeatDuration = time.Minute

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) Queue
	CollectStats(queueList []string) Stats
	GetOpenQueues() []string
}

// RedisConnection is the entry point. Use a connection to access queues, consumers and deliveries
// Each connection has a single heartbeat shared among all consumers
type RedisConnection struct {
	Name             string
	heartbeatKey     string // key to keep alive
	queuesKey        string // key to list of queues consumed by this connection
	redisClient      redis.Cmdable
	heartbeatStopped bool
}

// OpenConnectionWithRedisCmdable opens and returns a new connection
func OpenConnectionWithRedisCmdable(tag string, redisClient redis.Cmdable) *RedisConnection {
	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	connection := &RedisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
	}

	if !connection.updateHeartbeat() { // checks the connection
		log.Panicf("rmq connection failed to update heartbeat %s", connection)
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	redisErrIsNil(redisClient.SAdd(connectionsKey, name))

	go connection.heartbeat()
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, address string, db int) *RedisConnection {
	redisClient := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	return OpenConnectionWithRedisCmdable(tag, redisClient)
}

// OpenClusterConnection opens and returns a new connection to a Redis Cluster
func OpenClusterConnection(tag string, addresses []string) *RedisConnection {
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses,
	})
	return OpenConnectionWithRedisCmdable(tag, redisClient)
}

// OpenQueue opens and returns the queue with a given name
func (connection *RedisConnection) OpenQueue(name string) Queue {
	redisErrIsNil(connection.redisClient.SAdd(queuesKey, name))
	queue := newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
	return queue
}

// CollectStats returns a populated Stats object for all RMQ queues visible to
// the connection.
func (connection *RedisConnection) CollectStats(queueList []string) Stats {
	return collectStats(queueList, connection)
}

// String returns the connection name
func (connection *RedisConnection) String() string {
	return connection.Name
}

// GetConnections returns a list of all open connections
func (connection *RedisConnection) GetConnections() []string {
	result := connection.redisClient.SMembers(connectionsKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *RedisConnection) Check() bool {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	result := connection.redisClient.TTL(heartbeatKey)
	if redisErrIsNil(result) {
		return false
	}
	return result.Val() > 0
}

// StopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *RedisConnection) StopHeartbeat() bool {
	connection.heartbeatStopped = true
	return !redisErrIsNil(connection.redisClient.Del(connection.heartbeatKey))
}

// Close safely shuts down the client and removes the active connection from the
// set of active RMQ connections
func (connection *RedisConnection) Close() bool {
	return !redisErrIsNil(connection.redisClient.SRem(connectionsKey, connection.Name))
}

// GetOpenQueues returns a list of all open queues
func (connection *RedisConnection) GetOpenQueues() []string {
	result := connection.redisClient.SMembers(queuesKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// CloseAllQueues closes all queues by removing them from the global list
func (connection *RedisConnection) CloseAllQueues() int {
	result := connection.redisClient.Del(queuesKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

// CloseAllQueuesInConnection closes all queues in the associated connection by removing all related keys
func (connection *RedisConnection) CloseAllQueuesInConnection() error {
	redisErrIsNil(connection.redisClient.Del(connection.queuesKey))
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
	return nil
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *RedisConnection) GetConsumingQueues() []string {
	result := connection.redisClient.SMembers(connection.queuesKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// heartbeat keeps the heartbeat key alive
func (connection *RedisConnection) heartbeat() {
	for {
		if !connection.updateHeartbeat() {
			// log.Printf("rmq connection failed to update heartbeat %s", connection)
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			// log.Printf("rmq connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *RedisConnection) updateHeartbeat() bool {
	return !redisErrIsNil(connection.redisClient.Set(connection.heartbeatKey, "1", heartbeatDuration))
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *RedisConnection) hijackConnection(name string) *RedisConnection {
	return &RedisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// openQueue opens a queue without adding it to the set of queues
func (connection *RedisConnection) openQueue(name string) *redisQueue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *RedisConnection) flushDb() {
	connection.redisClient.FlushDb()
}
