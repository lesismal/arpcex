package main

import (
	"github.com/lesismal/arpc"
	"github.com/lesismal/arpc/log"
	"github.com/lesismal/arpcex/extension/middleware/coder/msgpack"
)

func main() {
	svr := arpc.NewServer()

	svr.Handler.UseCoder(msgpack.New())

	// register router
	svr.Handler.Handle("/echo", func(ctx *arpc.Context) {
		ctx.Write(ctx.Body())
		log.Info("/echo, %v", ctx.Values())
	})

	svr.Run("localhost:8888")
}
