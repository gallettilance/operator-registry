package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/operator-framework/operator-registry/pkg/registry"
)

type SQLLoader struct {
	db       *sql.DB
	migrator Migrator
}

var _ registry.Load = &SQLLoader{}

func NewSQLLiteLoader(db *sql.DB, opts ...DbOption) (*SQLLoader, error) {
	options := defaultDBOptions()
	for _, o := range opts {
		o(options)
	}

	if _, err := db.Exec("PRAGMA foreign_keys = ON", nil); err != nil {
		return nil, err
	}

	migrator, err := options.MigratorBuilder(db)
	if err != nil {
		return nil, err
	}

	return &SQLLoader{db: db, migrator: migrator}, nil
}

func (s *SQLLoader) Migrate(ctx context.Context) error {
	if s.migrator == nil {
		return fmt.Errorf("no migrator configured")
	}
	return s.migrator.Migrate(ctx)
}

func (s *SQLLoader) AddOperatorBundle(bundle *registry.Bundle) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()

	stmt, err := tx.Prepare("insert into operatorbundle(name, csv, bundle) values(?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	addImage, err := tx.Prepare("insert into related_image(image, operatorbundle_name) values(?,?)")
	if err != nil {
		return err
	}
	defer addImage.Close()

	csvName, csvBytes, bundleBytes, err := bundle.Serialize()
	if err != nil {
		return err
	}

	if csvName == "" {
		return fmt.Errorf("csv name not found")
	}

	if _, err := stmt.Exec(csvName, csvBytes, bundleBytes); err != nil {
		return err
	}

	imgs, err := bundle.Images()
	if err != nil {
		return err
	}
	// TODO: bulk insert
	for img := range imgs {
		if _, err := addImage.Exec(img, csvName); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLLoader) AddPackageChannels(manifest registry.PackageManifest) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()

	addPackage, err := tx.Prepare("insert into package(name) values(?)")
	if err != nil {
		return err
	}
	defer addPackage.Close()

	addDefaultChannel, err := tx.Prepare("update package set default_channel = ? where name = ?")
	if err != nil {
		return err
	}
	defer addDefaultChannel.Close()

	addChannel, err := tx.Prepare("insert into channel(name, package_name, head_operatorbundle_name) values(?, ?, ?)")
	if err != nil {
		return err
	}
	defer addChannel.Close()

	addChannelEntry, err := tx.Prepare("insert into channel_entry(channel_name, package_name, operatorbundle_name, depth) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addChannelEntry.Close()

	addReplaces, err := tx.Prepare("update channel_entry set replaces = ? where entry_id = ?")
	if err != nil {
		return err
	}
	defer addReplaces.Close()

	addAPI, err := tx.Prepare("insert or replace into api(group_name, version, kind, plural) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addAPI.Close()

	if _, err := addPackage.Exec(manifest.PackageName); err != nil {
		err = s.updatePackageChannels(tx, manifest)
		if err != nil {
			return err
		}
		return nil
	}

	hasDefault := false
	var errs []error
	for _, c := range manifest.Channels {
		if _, err := addChannel.Exec(c.Name, manifest.PackageName, c.CurrentCSVName); err != nil {
			errs = append(errs, err)
			continue
		}
		if c.IsDefaultChannel(manifest) {
			hasDefault = true
			if _, err := addDefaultChannel.Exec(c.Name, manifest.PackageName); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	if !hasDefault {
		errs = append(errs, fmt.Errorf("no default channel specified for %s", manifest.PackageName))
	}

	for _, c := range manifest.Channels {
		res, err := addChannelEntry.Exec(c.Name, manifest.PackageName, c.CurrentCSVName, 0)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		currentID, err := res.LastInsertId()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		channelEntryCSVName := c.CurrentCSVName
		depth := 1
		for {

			// Get CSV for current entry
			channelEntryCSV, err := s.getCSV(tx, channelEntryCSVName)
			if err != nil {
				errs = append(errs, err)
				break
			}

			if err := s.addProvidedAPIs(tx, channelEntryCSV, currentID); err != nil {
				errs = append(errs, err)
			}

			skips, err := channelEntryCSV.GetSkips()
			if err != nil {
				errs = append(errs, err)
			}

			for _, skip := range skips {
				// add dummy channel entry for the skipped version
				skippedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, skip, depth)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				skippedID, err := skippedChannelEntry.LastInsertId()
				if err != nil {
					errs = append(errs, err)
					continue
				}

				// add another channel entry for the parent, which replaces the skipped
				synthesizedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, channelEntryCSVName, depth)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				synthesizedID, err := synthesizedChannelEntry.LastInsertId()
				if err != nil {
					errs = append(errs, err)
					continue
				}

				if _, err = addReplaces.Exec(skippedID, synthesizedID); err != nil {
					errs = append(errs, err)
					continue
				}

				if err := s.addProvidedAPIs(tx, channelEntryCSV, synthesizedID); err != nil {
					errs = append(errs, err)
					continue
				}

				depth++
			}

			// create real replacement chain
			replaces, err := channelEntryCSV.GetReplaces()
			if err != nil {
				errs = append(errs, err)
				break
			}

			if replaces == "" {
				// we've walked the channel until there was no replacement
				break
			}

			replacedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, replaces, depth)
			if err != nil {
				errs = append(errs, err)
				break
			}
			replacedID, err := replacedChannelEntry.LastInsertId()
			if err != nil {
				errs = append(errs, err)
				break
			}
			if _, err = addReplaces.Exec(replacedID, currentID); err != nil {
				errs = append(errs, err)
				break
			}
			if _, err := s.getCSV(tx, replaces); err != nil {
				errs = append(errs, fmt.Errorf("%s specifies replacement that couldn't be found", c.CurrentCSVName))
				break
			}

			currentID = replacedID
			channelEntryCSVName = replaces
			depth++
		}
	}

	if err := tx.Commit(); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

func SplitCRDName(crdName string) (plural, group string, err error) {
	pluralGroup := strings.SplitN(crdName, ".", 2)
	if len(pluralGroup) != 2 {
		err = fmt.Errorf("can't split bad CRD name %s", crdName)
		return
	}

	plural = pluralGroup[0]
	group = pluralGroup[1]
	return
}

func (s *SQLLoader) getCSV(tx *sql.Tx, csvName string) (*registry.ClusterServiceVersion, error) {
	getCSV, err := tx.Prepare(`
	  SELECT DISTINCT operatorbundle.csv 
	  FROM operatorbundle
	  WHERE operatorbundle.name=? LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer getCSV.Close()

	rows, err := getCSV.Query(csvName)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, fmt.Errorf("no bundle found for csv %s", csvName)
	}
	var csvStringSQL sql.NullString
	if err := rows.Scan(&csvStringSQL); err != nil {
		return nil, err
	}

	dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(csvStringSQL.String), 10)
	unst := &unstructured.Unstructured{}
	if err := dec.Decode(unst); err != nil {
		return nil, err
	}

	csv := &registry.ClusterServiceVersion{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unst.UnstructuredContent(), csv); err != nil {
		return nil, err
	}

	return csv, nil
}

func (s *SQLLoader) addProvidedAPIs(tx *sql.Tx, csv *registry.ClusterServiceVersion, channelEntryId int64) error {
	addAPI, err := tx.Prepare("insert or replace into api(group_name, version, kind, plural) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addAPI.Close()

	addAPIProvider, err := tx.Prepare("insert or replace into api_provider(group_name, version, kind, channel_entry_id) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addAPIProvider.Close()

	ownedCRDs, _, err := csv.GetCustomResourceDefintions()
	for _, crd := range ownedCRDs {
		plural, group, err := SplitCRDName(crd.Name)
		if err != nil {
			return err
		}
		if _, err := addAPI.Exec(group, crd.Version, crd.Kind, plural); err != nil {
			return err
		}
		if _, err := addAPIProvider.Exec(group, crd.Version, crd.Kind, channelEntryId); err != nil {
			return err
		}
	}

	ownedAPIs, _, err := csv.GetApiServiceDefinitions()
	for _, api := range ownedAPIs {
		if _, err := addAPI.Exec(api.Group, api.Version, api.Kind, api.Name); err != nil {
			return err
		}
		if _, err := addAPIProvider.Exec(api.Group, api.Version, api.Kind, channelEntryId); err != nil {
			return err
		}
	}
	return nil
}
func (s *SQLLoader) getCSVNames(tx *sql.Tx, packageName string) ([]string, error) {
	getID, err := tx.Prepare(`
	  SELECT DISTINCT channel_entry.operatorbundle_name
	  FROM channel_entry
	  WHERE channel_entry.package_name=?`)

	if err != nil {
		return nil, err
	}
	defer getID.Close()

	rows, err := getID.Query(packageName)
	if err != nil {
		return nil, err
	}

	var csvName string
	csvNames := []string{}
	for rows.Next() {
		err := rows.Scan(&csvName)
		if err != nil {
			return nil, err
		}
		csvNames = append(csvNames, csvName)
	}

	return csvNames, nil
}

func (s *SQLLoader) rmAPIs(tx *sql.Tx, csv *registry.ClusterServiceVersion) error {
	rmAPI, err := tx.Prepare("delete from api where group_name=? AND version=? AND kind=?")
	if err != nil {
		return err
	}
	defer rmAPI.Close()

	ownedCRDs, _, err := csv.GetCustomResourceDefintions()
	for _, crd := range ownedCRDs {
		_, group, err := SplitCRDName(crd.Name)
		if err != nil {
			return err
		}
		if _, err := rmAPI.Exec(group, crd.Version, crd.Kind); err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLLoader) RmPackageName(packageName string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()

	csvNames, err := s.getCSVNames(tx, packageName)
	if err != nil {
		return err
	}
	for _, csvName := range csvNames {
		csv, err := s.getCSV(tx, csvName)
		if csv != nil {
			err = s.rmAPIs(tx, csv)
			if err != nil {
				return err
			}
			err = s.rmBundle(tx, csvName)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (s *SQLLoader) rmBundle(tx *sql.Tx, csvName string) error {
	stmt, err := tx.Prepare("DELETE FROM operatorbundle WHERE operatorbundle.name=?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(csvName); err != nil {
		return err
	}

	return nil
}

func (s *SQLLoader) updatePackageChannels(tx *sql.Tx, manifest registry.PackageManifest) error {
	updateDefaultChannel, err := tx.Prepare("update package set default_channel = ? where name = ?")
	if err != nil {
		return err
	}
	defer updateDefaultChannel.Close()

	getDefaultChannel, err := tx.Prepare(`SELECT default_channel FROM package WHERE name = ? LIMIT 1`)
	if err != nil {
		return err
	}
	defer getDefaultChannel.Close()

	updateChannel, err := tx.Prepare("update channel set head_operatorbundle_name = ? where name = ? and package_name = ?")
	if err != nil {
		return err
	}
	defer updateChannel.Close()

	addChannelEntry, err := tx.Prepare("insert into channel_entry(channel_name, package_name, operatorbundle_name, depth) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addChannelEntry.Close()

	updateChannelEntry, err := tx.Prepare("update channel_entry set depth = ? where channel_name = ? and package_name = ? and operatorbundle_name = ?")
	if err != nil {
		return err
	}
	defer updateChannelEntry.Close()

	addReplaces, err := tx.Prepare("update channel_entry set replaces = ? where entry_id = ?")
	if err != nil {
		return err
	}
	defer addReplaces.Close()

	addAPI, err := tx.Prepare("insert or replace into api(group_name, version, kind, plural) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer addAPI.Close()

	getDepth, err := tx.Prepare(`
	  SELECT channel_entry.depth, channel_entry.entry_id
	  FROM channel_entry
	  WHERE channel_name = ? and package_name = ? and operatorbundle_name =?
	  LIMIT 1`)
	if err != nil {
		return err
	}
	defer getDepth.Close()

	hasDefault := false
	var errs []error
	for _, c := range manifest.Channels {
		if _, err := updateChannel.Exec(c.CurrentCSVName, c.Name, manifest.PackageName); err != nil {
			errs = append(errs, err)
			continue
		}
		if c.IsDefaultChannel(manifest) {
			hasDefault = true
			if _, err := updateDefaultChannel.Exec(c.Name, manifest.PackageName); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	if !hasDefault {
		// TODO: check if it had a default
		rows, err := getDefaultChannel.Query(manifest.PackageName)
		if err != nil {
			errs = append(errs, err)
		}

		var defaultChannel string
		if rows.Next() {
			err := rows.Scan(&defaultChannel)
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("no default channel specified for %s", manifest.PackageName))
		}
	}

	for _, c := range manifest.Channels {
		rows, err := getDepth.Query(c.Name, manifest.PackageName, c.CurrentCSVName)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var depth int64
		var currentID int64
		channelEntryCSVName := c.CurrentCSVName
		if rows.Next() {
			err := rows.Scan(&depth, &currentID)
			if err != nil {
				errs = append(errs, err)
				continue
			}
		} else {
			res, err := addChannelEntry.Exec(c.Name, manifest.PackageName, c.CurrentCSVName, 0)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			currentID, err = res.LastInsertId()
			if err != nil {
				errs = append(errs, err)
				continue
			}

			depth = 1
		}

		for {

			// Get CSV for current entry
			channelEntryCSV, err := s.getCSV(tx, channelEntryCSVName)
			if err != nil {
				errs = append(errs, err)
				break
			}

			if err := s.addProvidedAPIs(tx, channelEntryCSV, currentID); err != nil {
				errs = append(errs, err)
			}

			skips, err := channelEntryCSV.GetSkips()
			if err != nil {
				errs = append(errs, err)
			}

			for _, skip := range skips {
				// add dummy channel entry for the skipped version
				skippedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, skip, depth)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				skippedID, err := skippedChannelEntry.LastInsertId()
				if err != nil {
					errs = append(errs, err)
					continue
				}

				// add another channel entry for the parent, which replaces the skipped
				synthesizedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, channelEntryCSVName, depth)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				synthesizedID, err := synthesizedChannelEntry.LastInsertId()
				if err != nil {
					errs = append(errs, err)
					continue
				}

				if _, err = addReplaces.Exec(skippedID, synthesizedID); err != nil {
					errs = append(errs, err)
					continue
				}

				if err := s.addProvidedAPIs(tx, channelEntryCSV, synthesizedID); err != nil {
					errs = append(errs, err)
					continue
				}

				depth++
			}

			// create real replacement chain
			replaces, err := channelEntryCSV.GetReplaces()
			if err != nil {
				errs = append(errs, err)
				break
			}

			if replaces == "" {
				// we've walked the channel until there was no replacement
				break
			}

			var replacedID int64
			_, err = updateChannelEntry.Exec(depth, c.Name, manifest.PackageName, replaces)
			if err != nil {

				replacedChannelEntry, err := addChannelEntry.Exec(c.Name, manifest.PackageName, replaces, depth)
				if err != nil {
					errs = append(errs, err)
					break
				}
				replacedID, err = replacedChannelEntry.LastInsertId()
				if err != nil {
					errs = append(errs, err)
					break
				}
				if _, err = addReplaces.Exec(replacedID, currentID); err != nil {
					errs = append(errs, err)
					break
				}
				if _, err := s.getCSV(tx, replaces); err != nil {
					errs = append(errs, fmt.Errorf("%s specifies replacement that couldn't be found", c.CurrentCSVName))
					break
				}
			} else {
				rows, err := getDepth.Query(c.Name, manifest.PackageName, replaces)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				var depth int64
				if rows.Next() {
					err := rows.Scan(&depth, &replacedID)
					if err != nil {
						errs = append(errs, err)
						continue
					}
				}
				if _, err = addReplaces.Exec(replacedID, currentID); err != nil {
					errs = append(errs, err)
					break
				}
			}

			currentID = replacedID
			channelEntryCSVName = replaces
			depth++
		}
	}

	if err := tx.Commit(); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}
