module ltstimewheel

go 1.20

require (
	github.com/judwhite/go-svc v0.0.0-00010101000000-000000000000
	github.com/julienschmidt/httprouter v1.3.0
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/rifflock/lfshook v0.0.0-20180920164130-b9218ef580f5
)

require (
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)

replace github.com/judwhite/go-svc => github.com/mreiferson/go-svc v1.2.2-0.20210815184239-7a96e00010f6
