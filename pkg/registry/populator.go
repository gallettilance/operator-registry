package registry

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/operator-framework/operator-registry/pkg/image"
)

type Dependencies struct {
	RawMessage []map[string]interface{} `json:"dependencies" yaml:"dependencies"`
}

// DirectoryPopulator loads an unpacked operator bundle from a directory into the database.
type DirectoryPopulator struct {
	loader      Load
	graphLoader GraphLoader
	querier     Query
	imageDirMap map[image.Reference]string
	overwrite   bool
}

func NewDirectoryPopulator(loader Load, graphLoader GraphLoader, querier Query, imageDirMap map[image.Reference]string, overwrite bool) *DirectoryPopulator {
	return &DirectoryPopulator{
		loader:      loader,
		graphLoader: graphLoader,
		querier:     querier,
		imageDirMap: imageDirMap,
		overwrite:   overwrite,
	}
}

func (i *DirectoryPopulator) Populate(mode Mode) error {
	var errs []error
	imagesToAdd := make([]*ImageInput, 0)
	for to, from := range i.imageDirMap {
		imageInput, err := NewImageInput(to, from)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		imagesToAdd = append(imagesToAdd, imageInput)
	}

	if len(errs) > 0 {
		return utilerrors.NewAggregate(errs)
	}

	err := i.loadManifests(imagesToAdd, mode)
	if err != nil {
		return err
	}

	return nil
}

func (i *DirectoryPopulator) globalSanityCheck(imagesToAdd []*ImageInput, mode Mode) (map[string]*ImageInput, error) {

	if i.overwrite && mode != ReplacesMode {
		return nil, OverwriteErr{ErrorString: "overwrite-latest flag is only supported in Replaces mode"}
	}

	var errs []error
	images := make(map[string]struct{})
	for _, image := range imagesToAdd {
		images[image.bundle.BundleImage] = struct{}{}
	}

	attemptedOverwritesPerPackage := make(map[string]*ImageInput)
	for _, image := range imagesToAdd {
		validOverwrite := false
		bundlePaths, err := i.querier.GetBundlePathsForPackage(context.TODO(), image.bundle.Package)
		if err != nil {
			// Assume that this means that the bundle is empty
			// Or that this is the first time the package is loaded.
			return nil, nil
		}
		for _, bundlePath := range bundlePaths {
			if _, ok := images[bundlePath]; ok {
				errs = append(errs, BundleImageAlreadyAddedErr{ErrorString: fmt.Sprintf("Bundle %s already exists", image.bundle.BundleImage)})
				continue
			}
		}

		channels, err := i.querier.ListChannels(context.TODO(), image.bundle.Package)
		if err != nil {
			errs = append(errs, err)
			return nil, utilerrors.NewAggregate(errs)
		}

		incomingChannels := make(map[string]struct{})
		for _, ch := range image.bundle.Channels {
			incomingChannels[ch] = struct{}{}
		}

		for _, channel := range channels {
			bundle, err := i.querier.GetBundle(context.TODO(), image.bundle.Package, channel, image.bundle.csv.GetName())
			if err != nil {
				// Assume that if we can not find a bundle for the package, channel and or CSV Name that this is safe to add
				continue
			}
			if bundle != nil {
				if !i.overwrite {
					// raise error that this package + channel + csv combo is already in the db
					errs = append(errs, PackageVersionAlreadyAddedErr{ErrorString: "Bundle already added that provides package and csv"})
					validOverwrite = false
					break
				}

				// ensure channels are the same
				if _, ok := incomingChannels[channel]; !ok {
					errs = append(errs, OverwriteErr{ErrorString: "channels must match when using --overwrite-latest"})
					validOverwrite = false
					break
				}

				// ensure replaces are the same
				replaces, err := image.bundle.csv.GetReplaces()
				if err != nil {
					errs = append(errs, err)
					return nil, utilerrors.NewAggregate(errs)
				}
				if bundle.GetReplaces() != replaces {
					errs = append(errs, OverwriteErr{ErrorString: fmt.Sprintf("replaces must match when using --overwrite-latest: got: %s want: %s", replaces, bundle.GetReplaces())})
					validOverwrite = false
					break
				}

				// ensure skips are the same
				skips, err := image.bundle.csv.GetSkips()
				if err != nil {
					errs = append(errs, err)
					return nil, utilerrors.NewAggregate(errs)
				}
				skipMap := make(map[string]struct{})
				for _, s := range skips {
					skipMap[s] = struct{}{}
				}
				for _, s := range bundle.GetSkips() {
					if s == "" {
						continue
					}
					if _, ok := skipMap[s]; !ok {
						errs = append(errs, OverwriteErr{ErrorString: "skips must match when using --overwrite-latest"})
						validOverwrite = false
						break
					}
				}

				// ensure skiprange is the same
				if bundle.GetSkipRange() != image.bundle.csv.GetSkipRange() {
					errs = append(errs, OverwriteErr{ErrorString: "skiprange must match when using --overwrite-latest"})
					validOverwrite = false
					break
				}
				// ensure default channel is set
				if image.annotationsFile.GetDefaultChannelName() == "" {
					errs = append(errs, OverwriteErr{ErrorString: "Must specify default channel when using --overwrite-latest"})
					continue
				}

				// ensure default channel is the same
				defaultChannel, err := i.querier.GetDefaultChannelForPackage(context.TODO(), image.bundle.Package)
				if err != nil {
					errs = append(errs, err)
					break
				}
				if defaultChannel != image.annotationsFile.GetDefaultChannelName() {
					errs = append(errs, OverwriteErr{ErrorString: "default channel must match when using --overwrite-latest"})
					break
				}
				// ensure overwrite is not in the middle of a channel (i.e. nothing replaces it)
				_, err = i.querier.GetBundleThatReplaces(context.TODO(), image.bundle.csv.GetName(), image.bundle.Package, channel)
				if err != nil {
					if err.Error() == fmt.Errorf("no entry found for %s %s", image.bundle.Package, channel).Error() {
						// overwrite is not replaced by any other bundle
						validOverwrite = true
						continue
					}
					errs = append(errs, err)
					break
				}
				// This bundle is in this channel but is not the head of this channel
				errs = append(errs, OverwriteErr{ErrorString: "Cannot overwrite a bundle that is not at the head of a channel using --overwrite-latest"})
				validOverwrite = false
				break
			}
		}
		if i.overwrite {
			if validOverwrite {
				if _, ok := attemptedOverwritesPerPackage[image.bundle.Package]; ok {
					errs = append(errs, OverwriteErr{ErrorString: "Cannot overwrite more than one bundle at a time for a given package using --overwrite-latest"})
					break
				}
				attemptedOverwritesPerPackage[image.bundle.Package] = image
			}
		}
	}

	return attemptedOverwritesPerPackage, utilerrors.NewAggregate(errs)
}

func (i *DirectoryPopulator) loadManifests(imagesToAdd []*ImageInput, mode Mode) error {
	// global sanity checks before insertion
	overwrites, err := i.globalSanityCheck(imagesToAdd, mode)
	if err != nil {
		return err
	}

	switch mode {
	case ReplacesMode:
		// TODO: This is relatively inefficient. Ideally, we should be able to use a replaces
		// graph loader to construct what the graph would look like with a set of new bundles
		// and use that to return an error if it's not valid, rather than insert one at a time
		// and reinspect the database.
		//
		// Additionally, it would be preferrable if there was a single database transaction
		// that took the updated graph as a whole as input, rather than inserting bundles of the
		// same package linearly.
		var err error
		var nonOverwriteImagesToAdd []*ImageInput
		var validImagesToAdd []*ImageInput

		for _, img := range imagesToAdd {
			if _, ok := overwrites[img.bundle.Package]; ok {
				if img.bundle.csv.GetName() == overwrites[img.bundle.Package].bundle.csv.GetName() {
					continue
				}
			}
			nonOverwriteImagesToAdd = append(nonOverwriteImagesToAdd, img)
		}

		// Add the overwriting bundles first
		for _, overwriteBundle := range overwrites {
			// Remove existing csv
			err = i.loader.ClearBundle(overwriteBundle.bundle.Package, overwriteBundle.bundle.csv.GetName())
			if err != nil {
				return err
			}
			err = i.loader.AddOperatorBundle(overwriteBundle.bundle)
			if err != nil {
				return err
			}
		}

		for len(nonOverwriteImagesToAdd) > 0 {
			validImagesToAdd, nonOverwriteImagesToAdd, err = i.getNextReplacesImagesToAdd(nonOverwriteImagesToAdd)
			if err != nil {
				return err
			}
			for _, image := range validImagesToAdd {
				err := i.loadManifestsReplaces(image.bundle, image.annotationsFile)
				if err != nil {
					return err
				}
			}
		}
	case SemVerMode:
		for _, image := range imagesToAdd {
			err := i.loadManifestsSemver(image.bundle, image.annotationsFile, false)
			if err != nil {
				return err
			}
		}
	case SkipPatchMode:
		for _, image := range imagesToAdd {
			err := i.loadManifestsSemver(image.bundle, image.annotationsFile, true)
			if err != nil {
				return err
			}
		}
	default:
		err := fmt.Errorf("Unsupported update mode")
		if err != nil {
			return err
		}
	}

	// Finally let's delete all the old bundles
	if err := i.loader.ClearNonHeadBundles(); err != nil {
		return fmt.Errorf("Error deleting previous bundles: %s", err)
	}

	return nil
}

// func (i *DirectoryPopulator) overwriteManifests(overwriteBundle *ImageInput) error {
// 	channels, err := i.querier.ListChannels(context.TODO(), overwriteBundle.annotationsFile.GetName())
// 	existingPackageChannels := map[string]string{}
// 	for _, c := range channels {
// 		current, err := i.querier.GetCurrentCSVNameForChannel(context.TODO(), overwriteBundle.annotationsFile.GetName(), c)
// 		if err != nil {
// 			return err
// 		}
// 		existingPackageChannels[c] = current
// 	}

// 	bcsv, err := overwriteBundle.bundle.ClusterServiceVersion()
// 	if err != nil {
// 		return fmt.Errorf("error getting csv from bundle %s: %s", overwriteBundle.bundle.Name, err)
// 	}

// 	packageManifest, err := translateAnnotationsIntoPackage(overwriteBundle.annotationsFile, bcsv, existingPackageChannels)
// 	if err != nil {
// 		return fmt.Errorf("Could not translate annotations file into packageManifest %s", err)
// 	}

// 	return i.loader.UpdateOperatorBundle(packageManifest, overwriteBundle.bundle)
// }

func (i *DirectoryPopulator) loadManifestsReplaces(bundle *Bundle, annotationsFile *AnnotationsFile) error {
	channels, err := i.querier.ListChannels(context.TODO(), annotationsFile.GetName())
	existingPackageChannels := map[string]string{}
	for _, c := range channels {
		current, err := i.querier.GetCurrentCSVNameForChannel(context.TODO(), annotationsFile.GetName(), c)
		if err != nil {
			return err
		}
		existingPackageChannels[c] = current
	}

	bcsv, err := bundle.ClusterServiceVersion()
	if err != nil {
		return fmt.Errorf("error getting csv from bundle %s: %s", bundle.Name, err)
	}

	packageManifest, err := translateAnnotationsIntoPackage(annotationsFile, bcsv, existingPackageChannels)
	if err != nil {
		return fmt.Errorf("Could not translate annotations file into packageManifest %s", err)
	}

	if err := i.loadOperatorBundle(packageManifest, bundle); err != nil {
		return fmt.Errorf("Error adding package %s", err)
	}

	return nil
}

func (i *DirectoryPopulator) getNextReplacesImagesToAdd(imagesToAdd []*ImageInput) ([]*ImageInput, []*ImageInput, error) {
	remainingImages := make([]*ImageInput, 0)
	foundImages := make([]*ImageInput, 0)

	var errs []error

	// Separate these image sets per package, since multiple different packages have
	// separate graph
	imagesPerPackage := make(map[string][]*ImageInput, 0)
	for _, image := range imagesToAdd {
		pkg := image.bundle.Package
		if _, ok := imagesPerPackage[pkg]; !ok {
			newPkgImages := make([]*ImageInput, 0)
			newPkgImages = append(newPkgImages, image)
			imagesPerPackage[pkg] = newPkgImages
		} else {
			imagesPerPackage[pkg] = append(imagesPerPackage[pkg], image)
		}
	}

	for pkg, pkgImages := range imagesPerPackage {
		// keep a tally of valid and invalid images to ensure at least one
		// image per package is valid. If not, throw an error
		pkgRemainingImages := 0
		pkgFoundImages := 0

		// first, try to pull the existing package graph from the database if it exists
		graph, err := i.graphLoader.Generate(pkg)
		if err != nil && !errors.Is(err, ErrPackageNotInDatabase) {
			return nil, nil, err
		}

		var pkgErrs []error
		// then check each image to see if it can be a replacement
		replacesLoader := ReplacesGraphLoader{}
		for _, pkgImage := range pkgImages {
			canAdd, err := replacesLoader.CanAdd(pkgImage.bundle, graph)
			if err != nil {
				pkgErrs = append(pkgErrs, err)
			}
			if canAdd {
				pkgFoundImages++
				foundImages = append(foundImages, pkgImage)
			} else {
				pkgRemainingImages++
				remainingImages = append(remainingImages, pkgImage)
			}
		}

		// no new images can be added, the current iteration aggregates all the
		// errors that describe invalid bundles
		if pkgFoundImages == 0 && pkgRemainingImages > 0 {
			errs = append(errs, utilerrors.NewAggregate(pkgErrs))
		}
	}

	if len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}

	return foundImages, remainingImages, nil
}

func (i *DirectoryPopulator) loadManifestsSemver(bundle *Bundle, annotations *AnnotationsFile, skippatch bool) error {
	graph, err := i.graphLoader.Generate(bundle.Package)
	if err != nil && !errors.Is(err, ErrPackageNotInDatabase) {
		return err
	}

	// add to the graph
	bundleLoader := BundleGraphLoader{}
	updatedGraph, err := bundleLoader.AddBundleToGraph(bundle, graph, annotations.Annotations.DefaultChannelName, skippatch)
	if err != nil {
		return err
	}

	if err := i.loader.AddBundleSemver(updatedGraph, bundle); err != nil {
		return fmt.Errorf("error loading bundle into db: %s", err)
	}

	return nil
}

// loadBundle takes the directory that a CSV is in and assumes the rest of the objects in that directory
// are part of the bundle.
func loadBundle(csvName string, dir string) (*Bundle, error) {
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

// findCSV looks through the bundle directory to find a csv
func (i *ImageInput) findCSV(manifests string) (*unstructured.Unstructured, error) {
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

// loadOperatorBundle adds the package information to the loader's store
func (i *DirectoryPopulator) loadOperatorBundle(manifest PackageManifest, bundle *Bundle) error {
	if manifest.PackageName == "" {
		return nil
	}

	if err := i.loader.AddBundlePackageChannels(manifest, bundle); err != nil {
		return fmt.Errorf("error loading bundle into db: %s", err)
	}

	return nil
}

// translateAnnotationsIntoPackage attempts to translate the channels.yaml file at the given path into a package.yaml
func translateAnnotationsIntoPackage(annotations *AnnotationsFile, csv *ClusterServiceVersion, existingPackageChannels map[string]string) (PackageManifest, error) {
	manifest := PackageManifest{}

	for _, ch := range annotations.GetChannels() {
		existingPackageChannels[ch] = csv.GetName()
	}

	channels := []PackageChannel{}
	for c, current := range existingPackageChannels {
		channels = append(channels,
			PackageChannel{
				Name:           c,
				CurrentCSVName: current,
			})
	}

	manifest = PackageManifest{
		PackageName:        annotations.GetName(),
		DefaultChannelName: annotations.GetDefaultChannelName(),
		Channels:           channels,
	}

	return manifest, nil
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
