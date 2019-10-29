# Mocking code in operator-registry

In order to write unit tests that can test individual functions without making requests to external sources or processes, a mocking framework is needed. For example, with the addition of `opm`, the operator-registry project now shells out to container tools (podman, docker cli). In order to unit test these, we have added a set of files that mock function calls through interfaces.

We are using [`gomock`](https://github.com/golang/mock) as a utility for the purpose of generating implementations of interfaces that can be used as part of object construction. If, when developing the operator-registry, you need to update any files that are mocked, just install the mockgen tool provided by `gomock`:

```bash
go get github.com/golang/mock/mockgen
```

Then run `mockgen` to regenerate the mock implementation of the interface. For example, to regenerate `CommandRunner` in `github.com/operator-framework/operator-registry/pkg/containertools/command.go`:

```bash
mockgen -destination=./pkg/containertools/mock/mock_command.go -package=mock -mock_names=CommandRunner=ContainerToolsCommandRunner github.com/operator-framework/operator-registry/pkg/containertools CommandRunner
```

To write new tests that use a mock of a new interface, use the provided methods included in the mock file. Ex:

```go
package containertools

import (
	"testing"

	"github.com/operator-framework/operator-registry/pkg/containertools/mock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestReadDockerLabels(t *testing.T) {
	controller := gomock.NewController(t)
    defer controller.Finish()

    mockCmd := mock.NewContainerToolsCommandRunner(controller)
	mockCmd.EXPECT().Pull(image).Return(nil) // method your thing calls
    
    newType := newTypeImplementation{
        cmd:mockCmd,
    }

    _, err := newType.Foo()
    require.NoError(t, err)
}
```
