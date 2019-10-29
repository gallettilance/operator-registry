package containertools

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	imageManifestName = "manifest.json"
)

// imageManifest is the object format of container image manifest files
// use this type to parse manifest.json files inside container image blobs
type imageManifest struct {
	Layers []string `json:”Layers”`
}

type BundleReader struct {
}

func NewBundleReader() *BundleReader {
	return &BundleReader{}
}

func (b *BundleReader) GetBundle(image, outputDir string) error {
	r := NewCommandRunner(Podman)

	// Create the output directory if it doesn't exist
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.Mkdir(outputDir, 0777)
	}

	err := r.Pull(image)
	if err != nil {
		return err
	}

	workingDir, err := ioutil.TempDir("./", "bundle_staging_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workingDir)

	rootTarfile := filepath.Join(workingDir, "bundle.tar")

	err = r.Save(image, rootTarfile)
	if err != nil {
		return err
	}

	f, err := os.Open(rootTarfile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read the manifest.json file to find the right embedded tarball
	layerTarballs, err := getManifestLayers(tar.NewReader(f))
	if err != nil {
		return err
	}
// Untar theimage layer tarballs and push the bundle manifests to the output directory
	for _, tarball := range layerTarballs {
		f, err = os.Open(rootTarfile)
		if err != nil {
			return err
		}
		defer f.Close()

		err = extractBundleManifests(tarball, outputDir, tar.NewReader(f))
		if err != nil {
			return err
		}
	}

	return nil
}

func getManifestLayers(tarReader *tar.Reader) ([]string, error) {
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("invalid bundle image: unable to find manifest.json")
			}
			return nil, err
		}

		if header.Name == imageManifestName {
			buf := new(bytes.Buffer)
			buf.ReadFrom(tarReader)
			b := buf.Bytes()

			manifests := make([]imageManifest, 0)
			err := json.Unmarshal(b, &manifests)
			if err != nil {
				return nil, err
			}

			if len(manifests) == 0 {
				return nil, fmt.Errorf("invalid bundle image: manifest.json missing manifest data")
			}

			topManifest := manifests[0]

			if len(topManifest.Layers) == 0 {
				return nil, fmt.Errorf("invalid bundle image: manifest has no layers")
			}

			return topManifest.Layers, nil
		}
	}
}

func extractBundleManifests(layerTarball, outputDir string, tarReader *tar.Reader) error {
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("Manifest error: Layer tarball does not exist in bundle")
			}
			return err
		}

		if header.Typeflag == tar.TypeReg {
			if header.Name == layerTarball {
				// Found the embedded top layer tarball
				layerReader := tar.NewReader(tarReader)

				err = extractTarballToDir(outputDir, layerReader)
				if err != nil {
					return err
				}
			}

			continue
		} else {
			return nil
		}
	}
}

func extractTarballToDir(outputDir string, tarReader *tar.Reader) error {
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Create the directory if it doesn't exist
			directoryToWrite := filepath.Join(outputDir, header.Name)
			if _, err := os.Stat(directoryToWrite); os.IsNotExist(err) {
				os.Mkdir(directoryToWrite, 0777)
			}
		case tar.TypeReg:
			buf := new(bytes.Buffer)
			buf.ReadFrom(tarReader)
			b := buf.Bytes()

			manifestToWrite := filepath.Join(outputDir, header.Name)

			m, err := os.Create(manifestToWrite)
			if err != nil {
				return err
			}
			defer m.Close()

			_, err = m.Write(b)
			if err != nil {
				return err
			}
		}
	}
}
