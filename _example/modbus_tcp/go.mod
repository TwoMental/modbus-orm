module example

go 1.24.0

require (
	github.com/TwoMental/modbus v0.0.0-20251222094154-1884820c6aee
	github.com/TwoMental/modbus-orm v0.0.1
	github.com/tbrandon/mbserver v0.0.0-20231208015628-36eb59221ac2
)

require (
	github.com/goburrow/serial v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
)

replace github.com/TwoMental/modbus-orm => ../../
