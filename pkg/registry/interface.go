package registry

import (
	"context"
)

type Load interface {
	AddOperatorBundle(bundle *Bundle) error
	AddPackageChannels(manifest PackageManifest) error
	RmPackageName(packageName string) error
}

type Query interface {
	ListTables(ctx context.Context) ([]string, error)
	ListPackages(ctx context.Context) ([]string, error)
	GetPackage(ctx context.Context, name string) (*PackageManifest, error)
	GetBundle(ctx context.Context, pkgName, channelName, csvName string) (string, string, error)
	GetBundleForChannel(ctx context.Context, pkgName string, channelName string) (string, string, error)
	// Get all channel entries that say they replace this one
	GetChannelEntriesThatReplace(ctx context.Context, name string) (entries []*ChannelEntry, err error)
	// Get the bundle in a package/channel that replace this one
	GetBundleThatReplaces(ctx context.Context, name, pkgName, channelName string) (string, string, error)
	// Get all channel entries that provide an api
	GetChannelEntriesThatProvide(ctx context.Context, group, version, kind string) (entries []*ChannelEntry, err error)
	// Get latest channel entries that provide an api
	GetLatestChannelEntriesThatProvide(ctx context.Context, group, version, kind string) (entries []*ChannelEntry, err error)
	// Get the the latest bundle that provides the API in a default channel
	GetBundleThatProvides(ctx context.Context, group, version, kind string) (string, string, *ChannelEntry, error)
	// List all images in the database
	ListImages(ctx context.Context) ([]string, error)
	// List all images for a particular bundle
	GetImagesForBundle(ctx context.Context, bundleName string) ([]string, error)
}
