package rmq

import (
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestStatsSuite(t *testing.T) {
	TestingSuiteT(&StatsSuite{}, t)
}

type StatsSuite struct{}

func (suite *StatsSuite) TestStats(c *C) {
	connection := OpenConnection("stats-conn", "localhost:6379", 1)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	conn1 := OpenConnection("stats-conn1", "localhost:6379", 1)
	conn2 := OpenConnection("stats-conn2", "localhost:6379", 1)
	q1 := conn2.OpenQueue("stats-q1").(*redisQueue)
	q1.PurgeReady()
	q1.Publish("stats-d1")
	q2 := conn2.OpenQueue("stats-q2").(*redisQueue)
	q2.PurgeReady()
	consumer := NewTestConsumer("hand-A")
	consumer.AutoAck = false
	q2.StartConsuming(10, time.Millisecond)
	q2.AddConsumer("stats-cons1", consumer)
	q2.Publish("stats-d2")
	q2.Publish("stats-d3")
	q2.Publish("stats-d4")
	time.Sleep(2 * time.Millisecond)
	consumer.LastDeliveries[0].Ack()
	consumer.LastDeliveries[1].Reject()
	q2.AddConsumer("stats-cons2", NewTestConsumer("hand-B"))

	stats := connection.CollectStats([]string{"stats-q1", "stats-q2"})
	for key := range stats.QueueStats {
		c.Check(key, Matches, "stats.*")
	}

	q2.StopConsuming()
	connection.StopHeartbeat()
	conn1.StopHeartbeat()
	conn2.StopHeartbeat()
}
