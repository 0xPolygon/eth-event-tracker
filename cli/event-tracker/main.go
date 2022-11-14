package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/umbracle/ethgo"
	"github.com/umbracle/ethgo/jsonrpc"

	eventtracker "github.com/0xPolygon/eth-event-tracker"
	boltdbStore "github.com/0xPolygon/eth-event-tracker/boltdb"
)

func main() {
	var endpoint string
	var target string
	var storage string
	var startBlock uint64

	flag.StringVar(&endpoint, "endpoint", "", "")
	flag.StringVar(&target, "target", "", "")
	flag.StringVar(&storage, "storage", "", "")
	flag.Uint64Var(&startBlock, "start-block", 0, "")

	flag.Parse()

	provider, err := jsonrpc.NewClient(endpoint)
	if err != nil {
		fmt.Printf("[ERR]: %v", err)
		os.Exit(1)
	}

	var entry eventtracker.Entry
	if storage == "" {
		entry = eventtracker.NewInmemStore()
	} else {
		store, err := boltdbStore.New(storage)
		if err != nil {
			fmt.Printf("[ERR]: failted to start store %v", err)
			os.Exit(1)
		}
		if entry, err = store.GetEntry(""); err != nil {
			fmt.Printf("[ERR]: failted to start entry %v", err)
			os.Exit(1)
		}
	}

	tt, err := eventtracker.NewTracker(provider.Eth(),
		eventtracker.WithBatchSize(20000),
		eventtracker.WithStore(entry),
		eventtracker.WithEtherscan(os.Getenv("ETHERSCAN_APIKEY")),
		eventtracker.WithStartBlock(startBlock),
		eventtracker.WithFilter(&eventtracker.FilterConfig{
			Address: []ethgo.Address{
				ethgo.HexToAddress(target),
			},
		}),
	)
	if err != nil {
		fmt.Printf("[ERR]: failed to create the tracker %v", err)
		os.Exit(1)
	}

	lastBlock, err := entry.GetLastBlock()
	if err != nil {
		fmt.Printf("[ERR]: failed to get last block %v", err)
		os.Exit(1)
	}
	if lastBlock != nil {
		fmt.Printf("Last block processed: %d\n", lastBlock.Number)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	go func() {
		signalCh := make(chan os.Signal, 4)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

		<-signalCh
		cancelFn()
	}()

	go func() {
		for {
			evnt := <-tt.EventCh()
			fmt.Printf("Last block: %d. Logs: Added (%d), Removed (%d)\n", evnt.Block.Number, len(evnt.Added), len(evnt.Removed))
		}
	}()

	if err := tt.Sync(ctx); err != nil {
		os.Exit(1)
	}
}
