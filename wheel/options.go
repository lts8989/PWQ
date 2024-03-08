package wheel

import "flag"

type Options struct {
	IntervalSec uint8
	FilterType  uint8
	RemainSec   uint16
	TaskSize    uint32
	HTTPAddress string
}

func WheelFlagSet(opts *Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("wheel", flag.ExitOnError)
	flagSet.Uint("interval-sec", uint(opts.IntervalSec), "check the task every N seconds")
	flagSet.Uint("filter-type", uint(opts.FilterType), "retrieve task filtering conditions from cache files(0=>'bloom' or 1=>'map')")
	flagSet.Uint("remain-sec", uint(opts.RemainSec), "if the task is less than N seconds from execution time, the tasks in the cache will not be written to temporary file")
	flagSet.Uint("task-size", uint(opts.TaskSize), "if the task size within a single slot is less than n, the task  in the cache will not be written to a temporary file")
	flagSet.String("http-address", opts.HTTPAddress, "address to listen on for HTTP clients (<addr>:<port> for TCP/IP or <path> for unix socket)")
	return flagSet
}
