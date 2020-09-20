package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/registry"
)

type Dependencies struct {
	RawMessage []map[string]interface{} `json:"dependencies" yaml:"dependencies"`
}

// DirectoryPopulator loads an unpacked operator bundle from a directory into the database.
type DirectoryPopulator struct {
	inputDatabase string
	loader        registry.Load
	graphLoader   registry.GraphLoader
	querier       registry.Query
	imageDirMap   map[image.Reference]string
}

func NewDirectoryPopulator(inputDatabase string, imageDirMap map[image.Reference]string) (*DirectoryPopulator, error) {
	return &DirectoryPopulator{
		inputDatabase: inputDatabase,
		imageDirMap:   imageDirMap,
	}, nil
}

func (i *DirectoryPopulator) Populate(mode registry.Mode) error {
	db, err := sql.Open("sqlite3", i.inputDatabase)
	if err != nil {
		return err
	}
	defer db.Close()

	dbLoader, err := NewSQLLiteLoader(db)
	if err != nil {
		return err
	}
	if err := dbLoader.Migrate(context.TODO()); err != nil {
		return err
	}

	graphLoader, err := NewSQLGraphLoaderFromDB(db)
	if err != nil {
		return err
	}

	i.loader = dbLoader
	i.graphLoader = graphLoader
	i.querier = NewSQLLiteQuerierFromDb(db)

	var errs []error
	imagesToAdd := make([]*registry.ImageInput, 0)
	for to, from := range i.imageDirMap {
		imageInput, err := registry.NewImageInput(to, from)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		imagesToAdd = append(imagesToAdd, imageInput)
	}

	if len(errs) > 0 {
		return utilerrors.NewAggregate(errs)
	}

	err = i.loadManifests(imagesToAdd, mode)
	if err != nil {
		return err
	}

	return nil
}

func (i *DirectoryPopulator) globalSanityCheck(imagesToAdd []*registry.ImageInput) error {
	var errs []error
	images := make(map[string]struct{})
	for _, image := range imagesToAdd {
		images[image.Bundle.BundleImage] = struct{}{}
	}

	for _, image := range imagesToAdd {
		bundlePaths, err := i.querier.GetBundlePathsForPackage(context.TODO(), image.Bundle.Package)
		if err != nil {
			// Assume that this means that the bundle is empty
			// Or that this is the first time the package is loaded.
			return nil
		}
		for _, bundlePath := range bundlePaths {
			if _, ok := images[bundlePath]; ok {
				errs = append(errs, registry.BundleImageAlreadyAddedErr{ErrorString: fmt.Sprintf("Bundle %s already exists", image.Bundle.BundleImage)})
				continue
			}
		}
		channels, err := i.querier.ListChannels(context.TODO(), image.Bundle.Package)
		if err != nil {
			return err
		}

		for _, channel := range channels {
			bundle, err := i.querier.GetBundle(context.TODO(), image.Bundle.Package, channel, image.Bundle.Csv.GetName())
			if err != nil {
				// Assume that if we can not find a bundle for the package, channel and or CSV Name that this is safe to add
				continue
			}
			if bundle != nil {
				// raise error that this package + channel + csv combo is already in the db
				errs = append(errs, registry.PackageVersionAlreadyAddedErr{ErrorString: "Bundle already added that provides package and csv"})
				break
			}
		}
	}

	return utilerrors.NewAggregate(errs)
}

func (i *DirectoryPopulator) loadManifests(imagesToAdd []*registry.ImageInput, mode registry.Mode) error {
	// global sanity checks before insertion
	err := i.globalSanityCheck(imagesToAdd)
	if err != nil {
		return err
	}

	switch mode {
	case registry.ReplacesMode:
		// TODO: This is relatively inefficient. Ideally, we should be able to use a replaces
		// graph loader to construct what the graph would look like with a set of new bundles
		// and use that to return an error if it's not valid, rather than insert one at a time
		// and reinspect the database.
		//
		// Additionally, it would be preferrable if there was a single database transaction
		// that took the updated graph as a whole as input, rather than inserting bundles of the
		// same package linearly.
		var err error
		var validImagesToAdd []*registry.ImageInput

		for len(imagesToAdd) > 0 {
			validImagesToAdd, imagesToAdd, err = i.getNextReplacesImagesToAdd(imagesToAdd)
			if err != nil {
				return err
			}
			for _, image := range validImagesToAdd {
				err := i.loadManifestsReplaces(image.Bundle, image.AnnotationsFile)
				if err != nil {
					return err
				}
			}
		}
	case registry.SemVerMode:
		for _, image := range imagesToAdd {
			err := i.loadManifestsSemver(image.Bundle, image.AnnotationsFile, false)
			if err != nil {
				return err
			}
		}
	case registry.SkipPatchMode:
		for _, image := range imagesToAdd {
			err := i.loadManifestsSemver(image.Bundle, image.AnnotationsFile, true)
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

func (i *DirectoryPopulator) loadManifestsReplaces(bundle *registry.Bundle, annotationsFile *registry.AnnotationsFile) error {
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

func (i *DirectoryPopulator) getNextReplacesImagesToAdd(imagesToAdd []*registry.ImageInput) ([]*registry.ImageInput, []*registry.ImageInput, error) {
	remainingImages := make([]*registry.ImageInput, 0)
	foundImages := make([]*registry.ImageInput, 0)

	var errs []error

	// Separate these image sets per package, since multiple different packages have
	// separate graph
	imagesPerPackage := make(map[string][]*registry.ImageInput, 0)
	for _, image := range imagesToAdd {
		pkg := image.Bundle.Package
		if _, ok := imagesPerPackage[pkg]; !ok {
			newPkgImages := make([]*registry.ImageInput, 0)
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
		if err != nil && !errors.Is(err, registry.ErrPackageNotInDatabase) {
			return nil, nil, err
		}

		var pkgErrs []error
		// then check each image to see if it can be a replacement
		replacesLoader := registry.ReplacesGraphLoader{}
		for _, pkgImage := range pkgImages {
			canAdd, err := replacesLoader.CanAdd(pkgImage.Bundle, graph)
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

func (i *DirectoryPopulator) loadManifestsSemver(bundle *registry.Bundle, annotations *registry.AnnotationsFile, skippatch bool) error {
	graph, err := i.graphLoader.Generate(bundle.Package)
	if err != nil && !errors.Is(err, registry.ErrPackageNotInDatabase) {
		return err
	}

	// add to the graph
	bundleLoader := registry.BundleGraphLoader{}
	updatedGraph, err := bundleLoader.AddBundleToGraph(bundle, graph, annotations.Annotations.DefaultChannelName, skippatch)
	if err != nil {
		return err
	}

	if err := i.loader.AddBundleSemver(updatedGraph, bundle); err != nil {
		return fmt.Errorf("error loading bundle into db: %s", err)
	}

	return nil
}

// loadOperatorBundle adds the package information to the loader's store
func (i *DirectoryPopulator) loadOperatorBundle(manifest registry.PackageManifest, bundle *registry.Bundle) error {
	if manifest.PackageName == "" {
		return nil
	}

	if err := i.loader.AddBundlePackageChannels(manifest, bundle); err != nil {
		return fmt.Errorf("error loading bundle into db: %s", err)
	}

	return nil
}

// translateAnnotationsIntoPackage attempts to translate the channels.yaml file at the given path into a package.yaml
func translateAnnotationsIntoPackage(annotations *registry.AnnotationsFile, csv *registry.ClusterServiceVersion, existingPackageChannels map[string]string) (registry.PackageManifest, error) {
	manifest := registry.PackageManifest{}

	for _, ch := range annotations.GetChannels() {
		existingPackageChannels[ch] = csv.GetName()
	}

	channels := []registry.PackageChannel{}
	for c, current := range existingPackageChannels {
		channels = append(channels,
			registry.PackageChannel{
				Name:           c,
				CurrentCSVName: current,
			})
	}

	manifest = registry.PackageManifest{
		PackageName:        annotations.GetName(),
		DefaultChannelName: annotations.GetDefaultChannelName(),
		Channels:           channels,
	}

	return manifest, nil
}
