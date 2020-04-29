package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"gitlab.com/NebulousLabs/Sia/modules"

	sia "gitlab.com/NebulousLabs/Sia/node/api/client"
)

var (
	// errNoFiles is the error that will be returned if the siasync directory on
	// the Sia network has not been created yet by the first upload.
	errNoFiles = errors.New("no such file or directory")
)

// SiaFolder is a folder that is synchronized to a Sia node.
type SiaFolder struct {
	path          string
	client        *sia.Client
	archive       bool
	includeHidden bool
	prefix        string
	watcher       *fsnotify.Watcher

	files map[string]string // files is a map of file paths to SHA256 checksums, used to reconcile file changes

	closeChan chan struct{}
}

// contains checks if a string exists in a []strings.
func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

// checkFile checks if a file's extension is included or excluded
// included takes precedence over excluded.
func checkFile(path string) (bool, error) {
	if include != "" {
		if contains(includeExtensions, strings.TrimLeft(filepath.Ext(path), ".")) {
			log.Debug("Found extension in include flag")
			return true, nil
		}
		log.Debug("Extension not found in include flag")
		return false, nil

	}

	if !includeHidden {
		// Exclude all files beginning with a dot e. g. ".hidden-file"
		if strings.HasPrefix(filepath.Base(path), ".") {
			log.Debug("Excluding " + path)
			return false, nil
		}
		// Exclude all files ending with a ~ e. g. "readme.md~"
		if strings.HasSuffix(filepath.Base(path), "~") {
			log.Debug("Excluding " + path)
			return false, nil
		}
	}

	if exclude != "" {
		if contains(excludeExtensions, strings.TrimLeft(filepath.Ext(path), ".")) {
			return false, nil
		}
		return true, nil
	}
	return true, nil
}

// NewSiafolder creates a new SiaFolder using the provided path and api
// address.
func NewSiafolder(path string, client *sia.Client) (*SiaFolder, error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	sf := &SiaFolder{
		path:          abspath,
		files:         make(map[string]string),
		closeChan:     make(chan struct{}),
		client:        client,
		archive:       archive,
		includeHidden: includeHidden,
		prefix:        prefix,
		watcher:       nil,
	}

	// watch for file changes
	if !syncOnly {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
		err = watcher.Add(abspath)
		if err != nil {
			return nil, err
		}

		sf.watcher = watcher
	}

	// walk the provided path, accumulating a slice of files to potentially
	// upload and adding any subdirectories to the watcher.
	err = filepath.Walk(abspath, func(walkpath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if walkpath == path {
			return nil
		}

		// Check if a Directory was found
		if f.IsDir() {
			// subdirectories must be added to the watcher.
			if sf.watcher != nil {
				sf.watcher.Add(walkpath)
			}
			sf.files[walkpath] = ""
			// Check if dir exists on Sia - if not create it.
			if !sf.isDirectory(walkpath) {
				relpath, err := filepath.Rel(sf.path, walkpath)
				// Ignore the sync prefix path
				if relpath == "." {
					return nil
				}
				if err != nil {
					log.WithFields(logrus.Fields{
						"file": walkpath,
					}).Error("Error getting relative path")
					return err
				}
				err = sf.client.RenterDirCreatePost(getSiaPath(relpath))
				if err != nil {
					log.WithFields(logrus.Fields{
						"file": walkpath,
					}).Error("Error creating not existing file on Sia")
					return err
				}
			}
			// If directory exists, remove files inside from Sia no longer existing locally
			if !archive {
				log.Info("Removing directories & files missing from local directory")
				err = sf.removeDeletedFiles(walkpath)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// File Found
		log.WithFields(logrus.Fields{
			"file": walkpath,
		}).Debug("Calculating checksum for file")
		checksum, err := checksumFile(walkpath)
		if err != nil {
			return err
		}
		sf.files[walkpath] = checksum
		return nil
	})
	if err != nil {
		return nil, err
	}

	log.Info("Uploading files missing from Sia")
	err = sf.uploadNonExisting()
	if err != nil {
		return nil, err
	}

	// remove files & directories that are in Sia but not in local directory
	if !archive {
		log.Info("Removing files & directories missing from local directory")
		err = sf.removeDeletedFiles(sf.prefix)
		if err != nil {
			return nil, err
		}
		err = sf.removeDeletedDirectories(sf.prefix)
		if err != nil {
			return nil, err
		}
	}

	// since there is no simple way to retrieve a sha256 checksum of a remote
	// file, this only works in size-only mode
	if sizeOnly {
		log.Info("Uploading changed files")
		err = sf.uploadChanged()
	}
	if err != nil {
		return nil, err
	}

	go sf.eventWatcher()

	return sf, nil
}

// newSiaPath is a wrapper for modules.NewSiaPath that just panics if there is
// an error
func newSiaPath(path string) (siaPath modules.SiaPath) {
	siaPath, err := modules.NewSiaPath(path)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Panic("Unable to create new SiaPath")
	}
	return siaPath
}

// checksumFile returns a sha256 checksum or size of a given file on disk
// depending on a options provided
func checksumFile(path string) (string, error) {
	var checksum string
	var err error

	if sizeOnly {
		checksum, err = sizeFile(path)
	} else {
		checksum, err = sha256File(path)
	}
	if err != nil {
		return "", err
	}
	return checksum, nil
}

// sha256File returns a sha256 checksum of a given file on disk.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return string(h.Sum(nil)), nil
}

// sizeFile returns the file size
func sizeFile(path string) (string, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	size := stat.Size()

	return strconv.FormatInt(size, 10), nil
}

// eventWatcher continuously listens on the SiaFolder's watcher channels and
// performs the necessary upload/delete operations.
func (sf *SiaFolder) eventWatcher() {
	if sf.watcher == nil {
		return
	}

	for {
		select {
		case <-sf.closeChan:
			return
		case event := <-sf.watcher.Events:
			filename := filepath.Clean(event.Name)
			f, err := os.Stat(filename)
			if err == nil && f.IsDir() {
				sf.watcher.Add(filename)
				sf.handleDirCreate(filename)
				continue
			}
			goodForWrite, err := checkFile(filename)
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("Error with checkFile")
			}
			if !goodForWrite {
				continue
			}

			// WRITE event, checksum the file and re-upload it if it has changed
			if event.Op&fsnotify.Write == fsnotify.Write {
				err = sf.handleFileWrite(filename)
				if err != nil {
					log.WithFields(logrus.Fields{
						"error": err.Error(),
					}).Error("Error with handleFileWrite")
				}
			}

			// RENAME event - handle as remove since an actual rename is handeled as REMOVE and CREATE
			if event.Op&fsnotify.Rename == fsnotify.Rename {
				log.WithFields(logrus.Fields{
					"filename": filename,
				}).Info("File removal detected, removing file")
				err = sf.handleRemove(filename)
				if err != nil {
					log.WithFields(logrus.Fields{
						"error": err.Error(),
					}).Error("Error with handleRemove")
				}
			}

			// REMOVE event
			if event.Op&fsnotify.Remove == fsnotify.Remove && !sf.archive {
				// Ignore multiple fired inode events by checking if the
				// to be removed file is still know by sf
				_, exists := sf.files[filename]
				if exists {
					log.WithFields(logrus.Fields{
						"filename": filename,
					}).Info("File removal detected, removing file")
					err = sf.handleRemove(filename)
					if err != nil {
						log.WithFields(logrus.Fields{
							"error": err.Error(),
						}).Error("Error with handleRemove")
					}
				}
			}

			// CREATE event
			if event.Op&fsnotify.Create == fsnotify.Create {
				log.WithFields(logrus.Fields{
					"filename": filename,
				}).Info("File creation detected, uploading file")
				uploadRetry(sf, filename)
			}
		case err := <-sf.watcher.Errors:
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("fsevents error")
			}
		}
	}
}

// uploadRetry attempts to reupload a file to Sia
func uploadRetry(sf *SiaFolder, filename string) {
	err := sf.handleCreate(filename)
	if err == nil {
		return
	}

	// If there was an error returned from handleCreate, sleep for 10s and then
	// remove and try again
	time.Sleep(10 * time.Second)

	// check if we have received create event for a file that is already in sia
	exists, err := sf.isFile(filename)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("Error with isFile")
	}
	if exists && !archive {
		err := sf.handleRemove(filename)
		if err != nil {
			log.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Error("Error with handleRemove")
		}
	}

	err = sf.handleCreate(filename)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("Error with handleCreate")
	}
}

// isDirectory checks if `file` given as relative path is a directory.
func (sf *SiaFolder) isDirectory(file string) bool {
	// err will be nil if file is a dir
	_, err := sf.client.RenterDirGet(getSiaPath(file))
	if err == nil {
		return true
	}

	return false
}

// isFile checks to see if the file exists on Sia
func (sf *SiaFolder) isFile(file string) (bool, error) {
	relpath, err := filepath.Rel(sf.path, file)
	if err != nil {
		return false, fmt.Errorf("error getting relative path: %v", err)
	}

	_, err = sf.client.RenterFileGet(getSiaPath(relpath))
	exists := true
	// TODO Refactor to propper error handling e.g. compare err.Error() or use errors.Is()
	if err != nil && strings.Contains(err.Error(), "no file known") {
		exists = false
	}
	if err != nil && strings.Contains(err.Error(), "unable to get the fileinfo") {
		exists = false
	}
	return exists, nil
}

// handleFileWrite handles a WRITE fsevent.
func (sf *SiaFolder) handleFileWrite(file string) error {
	checksum, err := checksumFile(file)
	if err != nil {
		return err
	}

	oldChecksum, exists := sf.files[file]
	if exists && oldChecksum != checksum {
		log.WithFields(logrus.Fields{
			"file": file,
		}).Info("Change in file detected, reuploading")
		sf.files[file] = checksum
		if !sf.archive {
			err = sf.handleRemove(file)
			if err != nil {
				return err
			}
		}
		err = sf.handleCreate(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close releases any resources allocated by a SiaFolder.
func (sf *SiaFolder) Close() error {
	close(sf.closeChan)
	if sf.watcher != nil {
		return sf.watcher.Close()
	}
	return nil
}

// getSiaPath returns a SiaPath for relative file name with prefix appended
func getSiaPath(relpath string) modules.SiaPath {
	return newSiaPath(filepath.Join(prefix, relpath))
}

// handleDirCreate handle a file creation event for a dir. `file` is a relative path
// to the file on the disk.
func (sf *SiaFolder) handleDirCreate(file string) error {
	abspath, err := filepath.Abs(file)
	if err != nil {
		return fmt.Errorf("error getting absolute path to create: %v", err)
	}
	relpath, err := filepath.Rel(sf.path, file)
	if err != nil {
		return fmt.Errorf("error getting relative path to create: %v", err)
	}

	log.WithFields(logrus.Fields{
		"abspath": abspath,
	}).Debug("Creating directory")

	if !dryRun {
		err = sf.client.RenterDirCreatePost(getSiaPath(relpath))
		if err != nil {
			return fmt.Errorf("error creating %v: %v", file, err)
		}
	}

	sf.files[file] = ""
	return nil
}

// handleCreate handles a file creation event. `file` is a relative path to the
// file on disk.
func (sf *SiaFolder) handleCreate(file string) error {
	abspath, err := filepath.Abs(file)
	if err != nil {
		return fmt.Errorf("error getting absolute path to upload: %v", err)
	}
	relpath, err := filepath.Rel(sf.path, file)
	if err != nil {
		return fmt.Errorf("error getting relative path to upload: %v", err)
	}

	log.WithFields(logrus.Fields{
		"abspath": abspath,
	}).Debug("Uploading file")

	if !dryRun {
		err = sf.client.RenterUploadPost(abspath, getSiaPath(relpath), dataPieces, parityPieces)
		if err != nil {
			// TODO This should be resolved by using err.Error() == siafile.ErrorPathOverload.Error()
			//      Unfortunately the string values of the Error() instances do currently not match.
			//      Alternative: Request Sia to support errors.Is(err, siafile.ErrorPathOverload)
			if strings.Contains(err.Error(), "a file or folder already exists at the specified path") {
				return nil
			}
			return fmt.Errorf("error uploading %v: %v", file, err)
		}
	}

	checksum, err := checksumFile(file)
	if err != nil {
		return err
	}
	sf.files[file] = checksum
	return nil
}

// handleRemove handles a file removal event.
func (sf *SiaFolder) handleRemove(file string) error {
	relpath, err := filepath.Rel(sf.path, file)
	if err != nil {
		return fmt.Errorf("error getting relative path to remove: %v", err)
	}

	log.WithFields(logrus.Fields{
		"file": file,
	}).Debug("Deleting file")

	if !dryRun {
		didRunSiaCmd := false
		if sf.isDirectory(relpath) {
			err = sf.client.RenterDirDeletePost(getSiaPath(relpath))
			didRunSiaCmd = true
		}

		if ok, _ := sf.isFile(file); ok {
			err = sf.client.RenterFileDeletePost(getSiaPath(relpath))
			didRunSiaCmd = true
		}

		if err != nil {
			return fmt.Errorf("error removing %v: %v", file, err)
		}

		if !didRunSiaCmd {
			log.WithFields(logrus.Fields{
				"file": file,
			}).Error("Unhandled delete event")
		}
	}

	delete(sf.files, file)
	return nil
}

// uploadNonExisting runs once and performs any uploads required to ensure
// every file in files is uploaded to the Sia node.
func (sf *SiaFolder) uploadNonExisting() error {
	renterFiles, err := sf.getSiaFiles(sf.prefix)
	if err != nil && !strings.Contains(err.Error(), errNoFiles.Error()) {
		return err
	}

	for file := range sf.files {
		relpath, err := filepath.Rel(sf.path, file)
		if err != nil {
			return err
		}

		// Ignore directories
		if sf.isDirectory(relpath) {
			continue
		}

		goodForWrite, err := checkFile(filepath.Clean(file))
		if err != nil {
			log.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Error("Error with checkFile")
		}
		if !goodForWrite {
			continue
		}

		if _, ok := renterFiles[getSiaPath(relpath)]; !ok {
			err := sf.handleCreate(file)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// uploadChanged runs once and performs any uploads of files where file size in
// Sia is different from local file
func (sf *SiaFolder) uploadChanged() error {
	renterFiles, err := sf.getSiaFiles(sf.prefix)
	if err != nil && !strings.Contains(err.Error(), errNoFiles.Error()) {
		return err
	}

	for file := range sf.files {
		goodForWrite, err := checkFile(filepath.Clean(file))
		if err != nil {
			log.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Error("Error with checkFile")
		}
		if !goodForWrite {
			continue
		}

		relpath, err := filepath.Rel(sf.path, file)
		if err != nil {
			return err
		}
		if siafile, ok := renterFiles[getSiaPath(relpath)]; ok {
			sf.files[file] = strconv.FormatInt(int64(siafile.Filesize), 10)
			// set file size to size in Sia and call handleFileWrite
			// if local file has different size it will reload file to Sia
			err := sf.handleFileWrite(file)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// removeDeletedFiles runs once and removes any files from Sia that don't exist in
// local directory anymore
func (sf *SiaFolder) removeDeletedFiles(path string) error {
	renterFiles, err := sf.getSiaFiles(path)
	if err != nil && !strings.Contains(err.Error(), errNoFiles.Error()) {
		return err
	}

	for siapath, siafile := range renterFiles {
		goodForWrite, err := checkFile(filepath.Clean(siafile.SiaPath.Path))
		if err != nil {
			log.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Error("Error with checkFile")
		}
		if !goodForWrite {
			continue
		}

		// sf.prefix
		sp := strings.Split(siapath.String(), "/")
		filePath := filepath.Join(sf.path, strings.Join(sp[1:], "/"))
		if _, ok := sf.files[filePath]; !ok {
			err = sf.handleRemove(filePath)
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("Error with handleRemove")
			}
		}
	}

	return nil
}

// removeDeletedDirectories runs once and removes any directories from Sia that don't exist in
// local directory anymore
func (sf *SiaFolder) removeDeletedDirectories(path string) error {
	renterDirectories, err := sf.getSiaDirectories(path)
	if err != nil {
		return err
	}

	for siapath := range renterDirectories {
		// sf.prefix
		sp := strings.Split(siapath.String(), "/")

		// If this is a subdirectory in current directory ascend to it
		if len(sp) > len(strings.Split(path, "/")) {
			sf.removeDeletedDirectories(strings.Join(sp, "/"))
		}

		filePath := filepath.Join(sf.path, strings.Join(sp[1:], "/"))
		if _, ok := sf.files[filePath]; !ok {
			err = sf.handleRemove(filePath)
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("Error with handleRemove")
			}
		}
	}

	return nil
}

// filters Sia remote files, only files that match prefix parameter are returned
func (sf *SiaFolder) getSiaFiles(path string) (map[modules.SiaPath]modules.FileInfo, error) {
	relpath, err := filepath.Rel(sf.path, path)
	if err != nil && path != sf.prefix {
		return nil, err
	}
	siaSyncDir, err := sf.client.RenterDirGet(getSiaPath(relpath))
	if err != nil {
		return nil, err
	}
	siaFiles := make(map[modules.SiaPath]modules.FileInfo)
	for _, file := range siaSyncDir.Files {
		siaFiles[file.SiaPath] = file
	}
	return siaFiles, nil
}

// filters Sia remote directories, only directories that match prefix parameter are returned
func (sf *SiaFolder) getSiaDirectories(path string) (map[modules.SiaPath]modules.DirectoryInfo, error) {
	// To get relative path remove the prefix
	lpx := len(strings.Split(sf.prefix, "/"))
	siaSyncDir, err := sf.client.RenterDirGet(getSiaPath(strings.Join(strings.Split(path, "/")[lpx:], "/")))
	if err != nil {
		return nil, err
	}
	siaDirectories := make(map[modules.SiaPath]modules.DirectoryInfo)
	for _, directory := range siaSyncDir.Directories {
		siaDirectories[directory.SiaPath] = directory
	}
	return siaDirectories, nil
}
