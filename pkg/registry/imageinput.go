package registry

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/operator-framework/operator-registry/pkg/image"
)

type ImageInput struct {
	manifestsDir     string
	metadataDir      string
	to               image.Reference
	from             string
	AnnotationsFile  *AnnotationsFile
	dependenciesFile *DependenciesFile
	Bundle           *Bundle
}

func NewImageInput(to image.Reference, from string) (*ImageInput, error) {
	var annotationsFound, dependenciesFound bool
	path := from
	manifests := filepath.Join(path, "manifests")
	metadata := filepath.Join(path, "metadata")
	// Get annotations file
	log := logrus.WithFields(logrus.Fields{"dir": from, "file": metadata, "load": "annotations"})
	files, err := ioutil.ReadDir(metadata)
	if err != nil {
		return nil, fmt.Errorf("unable to read directory %s: %s", metadata, err)
	}

	// Look for the metadata and manifests sub-directories to find the annotations.yaml
	// file that will inform how the manifests of the bundle should be loaded into the database.
	// If dependencies.yaml which contains operator dependencies in metadata directory
	// exists, parse and load it into the DB
	annotationsFile := &AnnotationsFile{}
	dependenciesFile := &DependenciesFile{}
	for _, f := range files {
		if !annotationsFound {
			err = DecodeFile(filepath.Join(metadata, f.Name()), annotationsFile)
			if err == nil && *annotationsFile != (AnnotationsFile{}) {
				annotationsFound = true
				continue
			}
		}

		if !dependenciesFound {
			err = DecodeFile(filepath.Join(metadata, f.Name()), &dependenciesFile)
			if err != nil {
				return nil, err
			}
			if len(dependenciesFile.Dependencies) > 0 {
				dependenciesFound = true
			}
		}
	}

	if !annotationsFound {
		return nil, fmt.Errorf("Could not find annotations file")
	}

	if !dependenciesFound {
		log.Info("Could not find optional dependencies file")
	}

	imageInput := &ImageInput{
		manifestsDir:     manifests,
		metadataDir:      metadata,
		to:               to,
		from:             from,
		AnnotationsFile:  annotationsFile,
		dependenciesFile: dependenciesFile,
	}

	err = imageInput.getBundleFromManifests()
	if err != nil {
		return nil, err
	}

	return imageInput, nil
}

func (i *ImageInput) getBundleFromManifests() error {
	log := logrus.WithFields(logrus.Fields{"dir": i.from, "file": i.manifestsDir, "load": "bundle"})

	csv, err := i.FindCSV(i.manifestsDir)
	if err != nil {
		return err
	}

	if csv.Object == nil {
		return fmt.Errorf("csv is empty: %s", err)
	}

	log.Info("found csv, loading bundle")

	csvName := csv.GetName()

	bundle, err := LoadBundle(csvName, i.manifestsDir)
	if err != nil {
		return fmt.Errorf("error loading objs in directory: %s", err)
	}

	if bundle == nil || bundle.Size() == 0 {
		return fmt.Errorf("no bundle objects found")
	}

	// set the bundleimage on the bundle
	bundle.BundleImage = i.to.String()
	// set the dependencies on the bundle
	bundle.Dependencies = i.dependenciesFile.GetDependencies()

	bundle.Name = csvName
	bundle.Package = i.AnnotationsFile.Annotations.PackageName
	bundle.Channels = strings.Split(i.AnnotationsFile.Annotations.Channels, ",")

	if err := bundle.AllProvidedAPIsInBundle(); err != nil {
		return fmt.Errorf("error checking provided apis in bundle %s: %s", bundle.Name, err)
	}

	i.Bundle = bundle

	return nil
}

// DecodeFile decodes the file at a path into the given interface.
func DecodeFile(path string, into interface{}) error {
	if into == nil {
		panic("programmer error: decode destination must be instantiated before decode")
	}

	fileReader, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("unable to read file %s: %s", path, err)
	}
	defer fileReader.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(fileReader, 30)

	return decoder.Decode(into)
}

// LoadBundle takes the directory that a CSV is in and assumes the rest of the objects in that directory
// are part of the bundle.
func LoadBundle(csvName string, dir string) (*Bundle, error) {
	log := logrus.WithFields(logrus.Fields{"dir": dir, "load": "bundle"})
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	bundle := &Bundle{
		Name: csvName,
	}
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

		log.Info("loading bundle file")
		var (
			obj  = &unstructured.Unstructured{}
			path = filepath.Join(dir, f.Name())
		)
		if err = DecodeFile(path, obj); err != nil {
			log.WithError(err).Debugf("could not decode file contents for %s", path)
			continue
		}

		// Don't include other CSVs in the bundle
		if obj.GetKind() == "ClusterServiceVersion" && obj.GetName() != csvName {
			continue
		}

		if obj.Object != nil {
			bundle.Add(obj)
		}
	}

	return bundle, nil
}

// FindCSV looks through the bundle directory to find a csv
func (i *ImageInput) FindCSV(manifests string) (*unstructured.Unstructured, error) {
	log := logrus.WithFields(logrus.Fields{"dir": i.from, "find": "csv"})

	files, err := ioutil.ReadDir(manifests)
	if err != nil {
		return nil, fmt.Errorf("unable to read directory %s: %s", manifests, err)
	}

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

		var (
			obj  = &unstructured.Unstructured{}
			path = filepath.Join(manifests, f.Name())
		)
		if err = DecodeFile(path, obj); err != nil {
			log.WithError(err).Debugf("could not decode file contents for %s", path)
			continue
		}

		if obj.GetKind() != clusterServiceVersionKind {
			continue
		}

		return obj, nil
	}

	return nil, fmt.Errorf("no csv found in bundle")
}
