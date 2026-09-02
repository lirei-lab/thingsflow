package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"flow-core/internal/alarmmaterializer"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/natsutil"
	"flow-core/internal/twinevents"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := dbpkg.Init()
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer dbpkg.Close()
	dbpkg.LoadKeyDictionary()

	// The connection reconnects for as long as this process lives
	// (natsutil.Options). The ClosedHandler is the backstop for the case that
	// policy cannot cover -- a close that is NOT our own shutdown: this process
	// is nothing but one subscription, so a closed connection means it can
	// never do its job again, and staying Running with a dead subscription is
	// precisely the failure that halted the data plane on 2026-08-28. Exiting
	// hands the problem to Kubernetes, which restarts the pod and resubscribes.
	nc, err := natsutil.Connect(env("NATS_URL", nats.DefaultURL), "thingsflow-alarm-materializer",
		nats.ClosedHandler(func(*nats.Conn) {
			select {
			case <-ctx.Done():
				// Our own Close() during shutdown -- expected, not a fault.
				return
			default:
			}
			log.Printf("FATAL nats connection closed while running; exiting so the pod restarts and resubscribes")
			os.Exit(1)
		}))
	if err != nil {
		log.Fatalf("nats connect failed: %v", err)
	}
	defer nc.Close()

	// Twin event journal (R4): the materializer is its own deployment with its
	// own NATS connection — initialize the twin-events publisher so alarm
	// inserts emit durable events (graceful disable if NATS is unreachable).
	twinevents.InitPublisher(env("NATS_URL", nats.DefaultURL), "tf.twin.events.>")

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("nats jetstream failed: %v", err)
	}

	repo := alarmmaterializer.PostgresRepository{DB: db}
	subject := env("ALARM_INTENT_SUBJECT", "tf.alarm.intent.>")
	queue := env("ALARM_MATERIALIZER_QUEUE_GROUP", "thingsflow-alarm-materializer")
	durable := env("ALARM_MATERIALIZER_DURABLE", "thingsflow-alarm-materializer")
	stream := env("ALARM_INTENT_STREAM", "TF_ALARMS")

	// Retry instead of exiting: on a fresh install this process starts before
	// the post-install hook has created TF_ALARMS, so the first subscribe fails
	// with "stream not found". Exiting made Kubernetes restart the pod until the
	// stream appeared, which converges but leaves a restart count and an Error
	// state on every new deployment -- indistinguishable, at a glance, from a
	// component that is actually broken.
	var sub *nats.Subscription
	for attempt := 1; ; attempt++ {
		var err error
		sub, err = js.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
			handleMessage(ctx, repo, msg)
		}, nats.Durable(durable), nats.ManualAck(), nats.AckExplicit(), nats.BindStream(stream))
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			log.Printf("alarm materializer: giving up subscribing during shutdown: %v", err)
			return
		default:
		}
		// Logged every time, not just once: a stream that never appears is an
		// operator problem, and silence would hide it behind a Running pod.
		log.Printf("nats subscribe failed (attempt %d, retrying in 5s): %v", attempt, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	defer sub.Drain()

	log.Printf("alarm materializer subscribed subject=%s queue=%s durable=%s stream=%s", subject, queue, durable, stream)
	<-ctx.Done()
	log.Printf("alarm materializer shutting down")
}

func handleMessage(parent context.Context, repo alarmmaterializer.Repository, msg *nats.Msg) {
	intent, err := alarmmaterializer.ParseIntent(msg.Data)
	if err != nil {
		log.Printf("WARN dropping invalid alarm intent: %v", err)
		_ = msg.Term()
		return
	}

	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	result, err := alarmmaterializer.ApplyIntent(ctx, repo, intent, time.Now().UnixMilli(), uuid.NewString)
	if err != nil {
		log.Printf("ERROR applying alarm intent tenant=%s device=%s type=%s action=%s: %v",
			intent.TenantID, intent.DeviceID, intent.AlarmType, intent.Action, err)
		_ = msg.Nak()
		return
	}

	log.Printf("alarm intent applied action=%s alarm_id=%s tenant=%s device=%s type=%s",
		result.Action, result.AlarmID, intent.TenantID, intent.DeviceID, intent.AlarmType)
	_ = msg.Ack()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
