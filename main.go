package main

import (
	"archive/tar"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	docker "github.com/fsouza/go-dockerclient"
	log "github.com/sirupsen/logrus"
)

func main() {

	imageID := os.Args[1]
	bucket := os.Args[2]
	filename := os.Args[3]

	fmt.Printf("Exporting image %s to s3://%s/%s", imageID, bucket, filename)

	client, err := docker.NewClientFromEnv()
	if err != nil {
		fmt.Printf("Error creating docker client: %v\n", err)
	}

	img, err := client.InspectImage(imageID)
	if err != nil {
		fmt.Printf("Error inspecting image %s: %v\n", imageID, err)
	}

	rootFs := img.RootFS
	for i, layer := range rootFs.Layers {
		fmt.Printf("Layer %d -> %s\n", i, layer)
	}

	last_idx := len(rootFs.Layers) - 1
	last_layer := rootFs.Layers[last_idx]
	layer_sha := strings.ReplaceAll(last_layer, "sha256:", "")

	r, w := io.Pipe()
	tr, tw := io.Pipe()

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)

	// Setup the S3 Upload Manager. Also see the SDK doc for the Upload Manager
	// for more information on configuring part size, and concurrency.
	//
	// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
	uploader := s3manager.NewUploader(sess)
	var wg sync.WaitGroup

	go func() {
		fmt.Printf("Exporting image...\n")
		wg.Add(1)
		opts := docker.ExportImageOptions{
			Name:         imageID,
			OutputStream: tw,
		}
		err := client.ExportImage(opts)
		if err != nil {
			fmt.Printf("Error exporting image %s: %v\n", imageID, err)
		}
		fmt.Printf("Done exporting image!\n")
		wg.Done()
		tw.Close()
	}()

	go func() {
		ListTarFiles(tr, w, layer_sha)
	}()

	fmt.Printf("Going to upload to S3...\n")
	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),

		// Can also use the `filepath` standard library package to modify the
		// filename as need for an S3 object key. Such as turning absolute path
		// to a relative path.
		Key: aws.String(filename),

		// The file to be uploaded. io.ReadSeeker is preferred as the Uploader
		// will be able to optimize memory when uploading large content. io.Reader
		// is supported, but will require buffering of the reader's bytes for
		// each part.
		Body: r,
	})
	if err != nil {
		// Print the error and exit.
		fmt.Printf("Unable to upload %q to %q, %v\n", filename, bucket, err)
	}

	fmt.Printf("Successfully uploaded %q to %q\n", filename, bucket)

}

func ListTarFiles(r io.Reader, w io.WriteCloser, layer_sha string) error {

	fmt.Printf("looking for layer %s\n", layer_sha)

	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		if strings.Contains(header.Name, ".tar") {
			fmt.Printf("Found layer tar: %s\n", header.Name)

			tmpfile, err := ioutil.TempFile("", "layer")
			if err != nil {
				log.Fatal(err)
			}

			defer os.Remove(tmpfile.Name())
			h := sha256.New()

			mw := io.MultiWriter(tmpfile, h)

			n, err := io.Copy(mw, tr)
			if err != nil {
				fmt.Printf("Error copying layer to writer: %v\n", err)
				return err
			}

			fmt.Printf("Extracted %d bytes for layer %s\n", n, header.Name)
			hexString := fmt.Sprintf("%x", h.Sum(nil))
			if hexString == layer_sha {
				fmt.Printf("\n\n\n\nI FOOUUUUUUUUNDDDD ITTTT: %s -> %s\n\n\n\n", header.Name, layer_sha)
				tmpfile.Seek(0, 0)
				nb, err := io.Copy(w, tmpfile)
				if err != nil {
					fmt.Printf("Error writing tmp file %s to S3: %v\n", tmpfile.Name, err)
				}
				fmt.Printf("Wrote %d bytes to S3\n", nb)
				tmpfile.Close()
				os.Remove(tmpfile.Name())
				w.Close()
				return nil
			}

			os.Remove(tmpfile.Name())

		}

		/*
			// the following switch could also be done using fi.Mode(), not sure if there
			// a benefit of using one vs. the other.
			// fi := header.FileInfo()

			// check the file type
			switch header.Typeflag {

			// if its a dir and it doesn't exist create it
			case tar.TypeDir:
				if _, err := os.Stat(target); err != nil {
					if err := os.MkdirAll(target, 0755); err != nil {
						return err
					}
				}

			// if it's a file create it
			case tar.TypeReg:
				f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
				if err != nil {
					return err
				}

				// copy over contents
				if _, err := io.Copy(f, tr); err != nil {
					return err
				}

				// manually close here after each file operation; defering would cause each file close
				// to wait until all operations have completed.
				f.Close()
			}*/

	}
}
