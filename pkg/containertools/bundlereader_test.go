package containertools

import (
	"testing"
	"io/ioutil"
	"os"

	"github.com/operator-framework/operator-registry/pkg/containertools/mock"

	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func GetBundleTest(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	containerTool := "podman"
	image := "quay.io/operator-framework/example-bundle"
	temp, err := ioutil.TempDir("./", "bundlereader_test_")
	require.NoError(t,err)
	defer func() {
		require.NoError(t, os.RemoveAll(temp))
	}()

	logger := logrus.NewEntry(logrus.New())
	mockCmd := mock.NewContainerToolsCommandRunner(controller)

	mockCmd.EXPECT().Pull(image).Return(nil)
	mockCmd.EXPECT().Save(image, gomock.Any()).Return(nil)
	mockCmd.EXPECT().GetToolName().Return(containerTool)

	reader := BundleReaderImpl{
		logger: logger,
		cmd: mockCmd,
	}

	err = reader.GetBundle(image, temp)
	require.NoError(t, err)
}