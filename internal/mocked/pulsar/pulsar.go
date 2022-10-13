package pulsar

//go:generate mockery --name Client --log-level "error" --srcpkg $GOPATH/pkg/mod/github.com/apache/pulsar-client-go@v0.9.0/pulsar --disable-version-string
//go:generate mockery --name Consumer --log-level "error" --srcpkg $GOPATH/pkg/mod/github.com/apache/pulsar-client-go@v0.9.0/pulsar --disable-version-string
//go:generate mockery --name Producer --log-level "error" --srcpkg $GOPATH/pkg/mod/github.com/apache/pulsar-client-go@v0.9.0/pulsar --disable-version-string
//go:generate mockery --name Message --log-level "error" --srcpkg $GOPATH/pkg/mod/github.com/apache/pulsar-client-go@v0.9.0/pulsar --disable-version-string
