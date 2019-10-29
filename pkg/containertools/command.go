package containertools

import (
	"os/exec"

	"github.com/sirupsen/logrus"
)

const (
	// Podman cli tool
	Podman = "podman"
	// Docker cli tool
	Docker = "docker"
)

// CommandRunner defines methods to shell out to common container tools
type CommandRunner interface {
	GetToolName() string
	Pull(image string) error
	Save(image, tarFile string) error
}

// CommandRunnerImpl is configured to select a container cli tool and execute commands with that
// tooling.
type CommandRunnerImpl struct{
	logger *logrus.Entry
	containerTool string
}

// NewCommandRunner takes the containerTool as an input string and returns a CommandRunner to
// run commands with that cli tool
func NewCommandRunner(containerTool string, logger *logrus.Entry) CommandRunner {
	r := CommandRunnerImpl{
		logger: logger,
	}

	switch containerTool {
	case Podman:
		r.containerTool = Podman
	case Docker:
		r.containerTool = Docker
	default:
		r.containerTool = Podman
	}

	return &r
}

// GetToolName returns the container tool this command runner is using
func (r *CommandRunnerImpl) GetToolName() string {
	return r.containerTool
}

// Pull takes a container image path hosted on a container registry and runs the pull command to
// download it onto the local environment
func (r *CommandRunnerImpl) Pull(image string) error {
	args := []string{"pull", image}

	command := exec.Command(r.containerTool, args...)

	r.logger.Infof("running %s pull", r.containerTool)
	r.logger.Debugf("%s", command.Args)

	err := command.Run()
	if err != nil {
		return err
	}

	return nil
}

// Save takes a local container image and runs the save commmand to convert the image into a specified
// tarball and push it to the local directory
func (r *CommandRunnerImpl) Save(image, tarFile string) error {
	args := []string{"save", image, "-o", tarFile}

	command := exec.Command(r.containerTool, args...)

	r.logger.Infof("running %s save", r.containerTool)
	r.logger.Debugf("%s", command.Args)

	err := command.Run()
	if err != nil {
		return err
	}

	return nil
}
