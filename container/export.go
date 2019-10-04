package main

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"sync"
	//"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	docker "github.com/fsouza/go-dockerclient"
)

func main() {

	containerID := os.Args[1]
	bucket := os.Args[2]
	filename := os.Args[3]

	fmt.Printf("Exporting container %s to s3://%s/%s", containerID, bucket, filename)

	client, err := docker.NewClientFromEnv()
	if err != nil {
		fmt.Printf("Error creating docker client: %v\n", err)
	}

	r, w := io.Pipe()
	tr, tw := io.Pipe()
	ww := io.MultiWriter(w, tw)

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
		fmt.Printf("Exporting container...\n")
		wg.Add(1)
		opts := docker.ExportContainerOptions{
			ID:           containerID,
			OutputStream: ww,
		}
		err := client.ExportContainer(opts)
		if err != nil {
			fmt.Printf("Error exporting container %s: %v\n", containerID, err)
		}
		fmt.Printf("Done exporting container!\n")
		wg.Done()
		w.Close()
	}()

	go func() { 
		ListTarFiles(tr)
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

func ListTarFiles(r io.Reader) error { 
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
		fmt.Printf("Saw %s\n", header.Name)

		/*
		target := filepath.Join(dst, header.Name)

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
		}
		*/
	}
}
