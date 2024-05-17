package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	observ "github.com/ovn-org/ovn-kubernetes/go-controller/observability-lib"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigc
		fmt.Println("Received a signal, terminating.")
		cancel()
	}()
	enableDecoder := flag.Bool("enable-enrichment", false, "Enrich samples with nbdb data.")
	flag.Parse()
	err := observ.ReadSamples(ctx, *enableDecoder)
	if err != nil {
		fmt.Println(err.Error())
	}
}
