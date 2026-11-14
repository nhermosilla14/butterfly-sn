// Command butterfly-sn runs the MQTT-SN to MQTT gateway.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nhermosilla14/butterfly-sn/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := app.Run(ctx, os.Args[1:], os.Stdout)
	stop()
	os.Exit(code)
}
