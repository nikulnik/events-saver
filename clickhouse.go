package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"time"
)

// Accumulates records and batch-inserts them when the number of records reaches batchSize.
type batchInserter struct {
	conn            driver.Conn
	insertCh        chan Event
	numberOfWorkers int
	// How many records to accumulate before insert.
	batchSize int

	// If not enough records have been accumulated before batchTTL has expired, all the exising
	// accumulated records are inserted.
	// Resets after each insert.
	batchTTL time.Duration
}

func NewBatchInserter(conn driver.Conn) batchInserter {
	i := batchInserter{
		conn:            conn,
		batchSize:       100000,
		insertCh:        make(chan Event, 10000),
		batchTTL:        time.Second * 300,
		numberOfWorkers: 2,
	}
	for j := 0; j < i.numberOfWorkers; j++ {
		go i.insertBatch()
	}
	return i
}

// Insert an event to batch. The batch will be inserted after the timeout or when it reaches the batchSize.
func (c *batchInserter) Insert(event Event) {
	c.insertCh <- event
}

func (c *batchInserter) insertBatch() error {
	batch, err := c.prepare()
	timer := time.NewTimer(c.batchTTL)
	counter := 0
	for {
		select {
		case <-timer.C: // Insert all the records when the timer ticks.
			err = batch.Send()
			if err != nil {
				log.Println(err)
				return err
			}
			batch, _ = c.prepare()
			timer.Reset(c.batchTTL)

		case event := <-c.insertCh: // An event received. Add it to the batch...
			err := batch.Append(
				time.Time(event.ClientTime),
				time.Time(event.ServerTime),
				event.IP,
				event.DeviceID,
				event.DeviceOs,
				event.Session,
				event.Sequence,
				event.Event,
				event.ParamInt,
				event.ParamStr,
			)
			if err != nil {
				return err
			}

			counter++
			if counter == c.batchSize { // Insert the data since the batch reached the value
				counter = 0
				err = batch.Send()
				if err != nil {
					return err
				}
				batch, _ = c.prepare()
				timer.Reset(c.batchTTL)
			}
		}
	}
}

func (c *batchInserter) prepare() (driver.Batch, error) {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO events")
	if err != nil {
		log.Printf("failed to prepare batch due to: %s", err)
		return nil, err
	}
	return batch, nil
}
