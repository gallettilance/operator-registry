package registry

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/operator-framework/operator-registry/pkg/sqlite"
)

func newRegistryRmCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "rm",
		Short: "remove operator from operator registry DB",
		Long:  `Remove operator from operator registry DB`,

		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},

		RunE: rm,
	}

	rootCmd.Flags().Bool("debug", false, "enable debug logging")
	rootCmd.Flags().StringP("database", "d", "bundles.db", "relative path to database file")
	rootCmd.Flags().StringP("packages", "o", "", "comma separated list of package names to be deleted")
	rootCmd.Flags().Bool("permissive", false, "allow registry load errors")

	return rootCmd
}

func rm(cmd *cobra.Command, args []string) error {
	fromFilename, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}
	packages, err := cmd.Flags().GetString("packages")
	if err != nil {
		return err
	}
	permissive, err := cmd.Flags().GetBool("permissive")
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", fromFilename)
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

	remover := sqlite.NewSQLRemoverForPackages(dbLoader, packages)
	if err := remover.Remove(); err != nil {
		err = fmt.Errorf("error deleting packages from database: %s", err)
		if !permissive {
			logrus.WithError(err).Fatal("permissive mode disabled")
			return err
		}
		logrus.WithError(err).Warn("permissive mode enabled")
	}

	return nil
}
