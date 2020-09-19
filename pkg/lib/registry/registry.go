package registry

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"github.com/operator-framework/operator-registry/pkg/image/execregistry"
	"github.com/operator-framework/operator-registry/pkg/lib/certs"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/operator-framework/operator-registry/pkg/sqlite"
)

type RegistryUpdater struct {
	Logger *logrus.Entry
}

type AddToRegistryRequest struct {
	Permissive    bool
	SkipTLS       bool
	CaFile        string
	InputDatabase string
	Bundles       []string
	Mode          registry.Mode
	ContainerTool containertools.ContainerTool
	Overwrite     bool
}

func (r RegistryUpdater) AddToRegistry(request AddToRegistryRequest) error {
	// add custom ca certs to resolver

	var reg image.Registry
	var rerr error
	switch request.ContainerTool {
	case containertools.NoneTool:
		rootCAs, err := certs.RootCAs(request.CaFile)
		if err != nil {
			return fmt.Errorf("failed to get RootCAs: %v", err)
		}
		reg, rerr = containerdregistry.NewRegistry(containerdregistry.SkipTLS(request.SkipTLS), containerdregistry.WithRootCAs(rootCAs))
	case containertools.PodmanTool:
		fallthrough
	case containertools.DockerTool:
		reg, rerr = execregistry.NewRegistry(request.ContainerTool, r.Logger, containertools.SkipTLS(request.SkipTLS))
	}
	if rerr != nil {
		return rerr
	}
	defer func() {
		if err := reg.Destroy(); err != nil {
			r.Logger.WithError(err).Warn("error destroying local cache")
		}
	}()

	simpleRefs := make([]image.Reference, 0)
	for _, ref := range request.Bundles {
		simpleRefs = append(simpleRefs, image.SimpleReference(ref))
	}

	if err := populate(context.TODO(), request.InputDatabase, reg, simpleRefs, request.Mode, request.Overwrite); err != nil {
		r.Logger.Debugf("unable to populate database: %s", err)

		if !request.Permissive {
			r.Logger.WithError(err).Error("permissive mode disabled")
			return err
		} else {
			r.Logger.WithError(err).Warn("permissive mode enabled")
		}
	}

	return nil
}

func unpackImage(ctx context.Context, reg image.Registry, ref image.Reference) (image.Reference, string, func() error, error) {
	var errs []error
	workingDir, err := ioutil.TempDir("./", "bundle_tmp")
	if err != nil {
		errs = append(errs, err)
	}

	if err = reg.Pull(ctx, ref); err != nil {
		errs = append(errs, err)
	}

	if err = reg.Unpack(ctx, ref, workingDir); err != nil {
		errs = append(errs, err)
	}

	cleanup := func() error { return os.RemoveAll(workingDir) }

	if len(errs) > 0 {
		return nil, "", cleanup, utilerrors.NewAggregate(errs)
	}
	return ref, workingDir, cleanup, nil
}

func populate(ctx context.Context, inputDatabase string, reg image.Registry, refs []image.Reference, mode registry.Mode, overwrite bool) error {
	unpackedImageMap := make(map[image.Reference]string, 0)
	for _, ref := range refs {
		to, from, cleanup, err := unpackImage(ctx, reg, ref)
		if err != nil {
			return err
		}
		unpackedImageMap[to] = from
		defer cleanup()
	}

	overwriteImageMap := make(map[string]map[image.Reference]string, 0)
	if overwrite {
		db, err := sql.Open("sqlite3", inputDatabase)
		if err != nil {
			return err
		}
		defer db.Close()

		dbLoader, err := sqlite.NewSQLLiteLoader(db)
		if err != nil {
			return err
		}
		if err := dbLoader.Migrate(context.TODO()); err != nil {
			return err
		}

		querier := sqlite.NewSQLLiteQuerierFromDb(db)
		// find all bundles that are attempting to overwrite
		for to, from := range unpackedImageMap {
			img, err := registry.NewImageInput(to, from)
			if err != nil {
				return err
			}
			overwritten, err := querier.GetBundlePathIfExists(context.TODO(), img.Bundle.Csv.GetName())
			if err != nil {
				return err
			}
			if overwritten != "" {
				// get all bundle paths for that package - we will re-add these to regenerate the graph
				bundles, err := querier.GetBundlesForPackage(context.TODO(), img.Bundle.Package)
				if err != nil {
					return err
				}
				for bundle := range bundles {
					if _, ok := overwriteImageMap[img.Bundle.Package]; !ok {
						overwriteImageMap[img.Bundle.Package] = make(map[image.Reference]string, 0)
					}
					if bundle.CsvName != img.Bundle.Csv.GetName() {
						to, from, cleanup, err := unpackImage(context.TODO(), reg, image.SimpleReference(bundle.BundlePath))
						if err != nil {
							return err
						}
						defer cleanup()
						overwriteImageMap[img.Bundle.Package][to] = from
					}
				}
			}
		}
	}

	populator, err := sqlite.NewDirectoryPopulator(inputDatabase, unpackedImageMap, overwriteImageMap, overwrite)
	if err != nil {
		return err
	}

	return populator.Populate(mode)
}

type DeleteFromRegistryRequest struct {
	Permissive    bool
	InputDatabase string
	Packages      []string
}

func (r RegistryUpdater) DeleteFromRegistry(request DeleteFromRegistryRequest) error {
	db, err := sql.Open("sqlite3", request.InputDatabase)
	if err != nil {
		return err
	}
	defer db.Close()

	dbLoader, err := sqlite.NewSQLLiteLoader(db)
	if err != nil {
		return err
	}
	if err := dbLoader.Migrate(context.TODO()); err != nil {
		return err
	}

	for _, pkg := range request.Packages {
		remover := sqlite.NewSQLRemoverForPackages(dbLoader, pkg)
		if err := remover.Remove(); err != nil {
			err = fmt.Errorf("error deleting packages from database: %s", err)
			if !request.Permissive {
				logrus.WithError(err).Fatal("permissive mode disabled")
				return err
			}
			logrus.WithError(err).Warn("permissive mode enabled")
		}
	}

	// remove any stranded bundles from the database
	// TODO: This is unnecessary if the db schema can prevent this orphaned data from existing
	remover := sqlite.NewSQLStrandedBundleRemover(dbLoader)
	if err := remover.Remove(); err != nil {
		return fmt.Errorf("error removing stranded packages from database: %s", err)
	}

	return nil
}

type PruneStrandedFromRegistryRequest struct {
	InputDatabase string
}

func (r RegistryUpdater) PruneStrandedFromRegistry(request PruneStrandedFromRegistryRequest) error {
	db, err := sql.Open("sqlite3", request.InputDatabase)
	if err != nil {
		return err
	}
	defer db.Close()

	dbLoader, err := sqlite.NewSQLLiteLoader(db)
	if err != nil {
		return err
	}
	if err := dbLoader.Migrate(context.TODO()); err != nil {
		return err
	}

	remover := sqlite.NewSQLStrandedBundleRemover(dbLoader)
	if err := remover.Remove(); err != nil {
		return fmt.Errorf("error removing stranded packages from database: %s", err)
	}

	return nil
}

type PruneFromRegistryRequest struct {
	Permissive    bool
	InputDatabase string
	Packages      []string
}

func (r RegistryUpdater) PruneFromRegistry(request PruneFromRegistryRequest) error {
	db, err := sql.Open("sqlite3", request.InputDatabase)
	if err != nil {
		return err
	}
	defer db.Close()

	dbLoader, err := sqlite.NewSQLLiteLoader(db)
	if err != nil {
		return err
	}
	if err := dbLoader.Migrate(context.TODO()); err != nil {
		return err
	}

	// get all the packages
	lister := sqlite.NewSQLLiteQuerierFromDb(db)
	packages, err := lister.ListPackages(context.TODO())
	if err != nil {
		return err
	}

	// make it inexpensive to find packages
	pkgMap := make(map[string]bool)
	for _, pkg := range request.Packages {
		pkgMap[pkg] = true
	}

	// prune packages from registry
	for _, pkg := range packages {
		if _, found := pkgMap[pkg]; !found {
			remover := sqlite.NewSQLRemoverForPackages(dbLoader, pkg)
			if err := remover.Remove(); err != nil {
				err = fmt.Errorf("error deleting packages from database: %s", err)
				if !request.Permissive {
					logrus.WithError(err).Fatal("permissive mode disabled")
					return err
				}
				logrus.WithError(err).Warn("permissive mode enabled")
			}
		}
	}

	return nil
}

type DeprecateFromRegistryRequest struct {
	Permissive    bool
	InputDatabase string
	Bundles       []string
}

func (r RegistryUpdater) DeprecateFromRegistry(request DeprecateFromRegistryRequest) error {
	db, err := sql.Open("sqlite3", request.InputDatabase)
	if err != nil {
		return err
	}
	defer db.Close()

	dbLoader, err := sqlite.NewSQLLiteLoader(db)
	if err != nil {
		return err
	}
	if err := dbLoader.Migrate(context.TODO()); err != nil {
		return fmt.Errorf("unable to migrate database: %s", err)
	}

	deprecator := sqlite.NewSQLDeprecatorForBundles(dbLoader, request.Bundles)
	if err := deprecator.Deprecate(); err != nil {
		r.Logger.Debugf("unable to deprecate bundles from database: %s", err)
		if !request.Permissive {
			r.Logger.WithError(err).Error("permissive mode disabled")
			return err
		}
		r.Logger.WithError(err).Warn("permissive mode enabled")
	}

	return nil
}
