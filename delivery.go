package rmq

import (
	"fmt"

	"gopkg.in/redis.v5"
)

// Delivery wraps an RMQ message returned from Redis. All Delivery messages should be acknowledged
// once by calling either the `Ack()`, `Reject()`, or `Push()` functions.
type Delivery interface {
	Payload() string
	PayloadBytes() []byte
	Ack() bool
	Reject() bool
	Push() bool
}

type wrapDelivery struct {
	payload     []byte
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient redis.Cmdable
}

func newDelivery(payload []byte, unackedKey, rejectedKey, pushKey string, redisClient redis.Cmdable) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return string(delivery.payload)
}

func (delivery *wrapDelivery) PayloadBytes() []byte {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if redisErrIsNil(result) {
		return false
	}

	return result.Val() == 1
}

func (delivery *wrapDelivery) Reject() bool {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() bool {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) bool {
	if redisErrIsNil(delivery.redisClient.LPush(key, delivery.payload)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
