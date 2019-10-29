package sqlite

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/registry"
)

// ImageLoader loads a bundle image of resources into the database
type ImageLoader struct {
	store     registry.Load
	image     string
	directory string
	reader containertools.BundleReader
	log *logrus.Entry
}

func NewSQLLoaderForImage(store registry.Load, image string) *ImageLoader {
	logger := logrus.WithField("img", image)
	return &ImageLoader{
		store:     store,
		image:     image,
		directory: "",
		log: logger,
		reader: containertools.NewBundleReader(logger),
	}
}

func (i *ImageLoader) Populate() error {
	workingDir, err := ioutil.TempDir("./", "bundle_tmp")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workingDir)

	err = i.reader.GetBundle(i.image, workingDir)
	if err != nil {
		return err
	}

	i.directory = workingDir

	i.log.Info("loading Bundle")
	errs := make([]error, 0)
	if err := i.LoadBundleFunc(workingDir); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

// LoadBundleFunc walks the directory. When it sees a `.clusterserviceversion.yaml` file, it
// attempts to load the surrounding files in the same directory as a bundle, and stores them in the
// db for querying
func (i *ImageLoader) LoadBundleFunc(path string) error {
	manifests := filepath.Join(path, "manifests")
	metadata := filepath.Join(path, "metadata")

	// Get annotations file
	log := logrus.WithFields(logrus.Fields{"dir": i.directory, "file": metadata, "load": "annotations"})
	files, err := ioutil.ReadDir(metadata)
	if err != nil {
		return fmt.Errorf("unable to read directory %s: %s", metadata, err)
	}

	annotationsFile := &registry.AnnotationsFile{}
	for _, f := range files {
		fileReader, err := os.Open(filepath.Join(metadata, f.Name()))
		if err != nil {
			return fmt.Errorf("unable to read file %s: %s", f.Name(), err)
		}
		decoder := yaml.NewYAMLOrJSONDecoder(fileReader, 30)
		err = decoder.Decode(&annotationsFile)
		if err != nil || *annotationsFile == (registry.AnnotationsFile{}) {
			continue
		} else {
			log.Info("found annotations file searching for csv")
		}
	}

	if *annotationsFile == (registry.AnnotationsFile{}) {
		return fmt.Errorf("Could not find annotations.yaml file")
	}

	err = i.loadManifests(manifests, annotationsFile)
	if err != nil {
		return err
	}

	return nil
}

func (i *ImageLoader) loadManifests(manifests string, annotationsFile *registry.AnnotationsFile) error {
	log := logrus.WithFields(logrus.Fields{"dir": i.directory, "file": manifests, "load": "bundle"})

	csv, err := i.findCSV(manifests)
	if err != nil {
		return err
	}

	if csv.Object == nil {
		return fmt.Errorf("csv is empty: %s", err)
	}

	log.Info("found csv, loading bundle")

	// TODO: Check channels against what's in the database vs in the bundle csv

	bundle, err := loadBundle(csv.GetName(), manifests)
	if err != nil {
		return fmt.Errorf("error loading objs in directory: %s", err)
	}

	if bundle == nil || bundle.Size() == 0 {
		return fmt.Errorf("no bundle objects found")
	}

	if err := bundle.AllProvidedAPIsInBundle(); err != nil {
		return fmt.Errorf("error checking provided apis in bundle %s: %s", bundle.Name, err)
	}

	if err := i.store.AddOperatorBundle(bundle); err != nil {
		return fmt.Errorf("error adding operator bundle %s: %s", bundle.Name, err)
	}

	bcsv, err := bundle.ClusterServiceVersion()
	if err != nil {
		return fmt.Errorf("error getting csv from bundle %s: %s", bundle.Name, err)
	}

	packageManifest, err := translateAnnotationsIntoPackage(annotationsFile, bcsv)
	if err != nil {
		return fmt.Errorf("Could not translate annotations file into packageManifest %s", err)
	}

	if err := i.loadPackages(packageManifest); err != nil {
		return fmt.Errorf("Error adding package %s", err)
	}

	return nil
}

// findCSV looks through the bundle directory to find a csv
func (i *ImageLoader) findCSV(manifests string) (*unstructured.Unstructured, error) {
	log := logrus.WithFields(logrus.Fields{"dir": i.directory, "find": "csv"})

	files, err := ioutil.ReadDir(manifests)
	if err != nil {
		return nil, fmt.Errorf("unable to read directory %s: %s", manifests, err)
	}

	var errs []error
	for _, f := range files {
		log = log.WithField("file", f.Name())
		if f.IsDir() {
			log.Info("skipping directory")
			continue
		}

		if strings.HasPrefix(f.Name(), ".") {
			log.Info("skipping hidden file")
			continue
		}

		path := filepath.Join(manifests, f.Name())
		fileReader, err := os.Open(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to read file %s: %s", path, err))
			continue
		}

		dec := yaml.NewYAMLOrJSONDecoder(fileReader, 30)
		unst := &unstructured.Unstructured{}
		if err := dec.Decode(unst); err != nil {
			continue
		}

		if unst.GetKind() != ClusterServiceVersionKind {
			continue
		}

		return unst, nil

	}

	errs = append(errs, fmt.Errorf("no csv found in bundle"))
	return nil, utilerrors.NewAggregate(errs)
}

// loadPackages adds the package information to the loader's store
func (i *ImageLoader) loadPackages(manifest registry.PackageManifest) error {
	if manifest.PackageName == "" {
		return nil
	}

	if err := i.store.AddPackageChannels(manifest); err != nil {
		return fmt.Errorf("error loading package into db: %s", err)
	}

	return nil
}

// translateAnnotationsIntoPackage attempts to translate the channels.yaml file at the given path into a package.yaml
func translateAnnotationsIntoPackage(annotations *registry.AnnotationsFile, csv *registry.ClusterServiceVersion) (registry.PackageManifest, error) {
	manifest := registry.PackageManifest{}

	channels := []registry.PackageChannel{}
	for _, ch := range annotations.GetChannels() {
		channels = append(channels,
			registry.PackageChannel{
				Name:           ch,
				CurrentCSVName: csv.GetName(),
			})
	}

	manifest = registry.PackageManifest{
		PackageName:        annotations.GetName(),
		DefaultChannelName: annotations.GetDefaultChannelName(),
		Channels:           channels,
	}

	return manifest, nil
}
