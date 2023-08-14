package s3lib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TODO fill these in!
const (
	//_PART_SIZE = 26_843_545_600 // Current 25MB. Has to be 5_000_000 minimim
	_PART_SIZE = 6_000_000 // Current 25MB. Has to be 5_000_000 minimim
	_RETRIES   = 2
)

type Config struct {
	S3Region         string
	Endpoint         string
	AccessKey        string
	SecretKey        string
	S3ForcePathStyle bool
}

type S3Object struct {
	Filename     string
	Etag         string
	LastModified time.Time
	Size         int64
}

type S3Session struct {
	*s3.S3
}

// GetSession creates a new AWS session. Can be reused to upload multiple files
func GetSession(cfg *Config) (*S3Session, error) {
	// Create a single AWS session (we can re use this if we're uploading many files)
	//s, err := session.NewSession(&aws.Config{Region: aws.String(S3_REGION)})

	awscfg := aws.Config{}
	awscfg.Region = aws.String(cfg.S3Region)
	if cfg.Endpoint != "" {
		awscfg.Endpoint = aws.String(cfg.Endpoint)
	}
	if cfg.S3ForcePathStyle {
		awscfg.S3ForcePathStyle = aws.Bool(true)
	}
	awscfg.Credentials = credentials.NewStaticCredentials(
		cfg.AccessKey,
		cfg.SecretKey,
		"", // a token will be created when the session it's used.
	)
	s, err := session.NewSession(&awscfg)

	// s, err := session.NewSession(
	// 	&aws.Config{
	// 		Region:   aws.String(cfg.S3Region),
	// 		Endpoint: aws.String(cfg.Endpoint),
	// 		Credentials: credentials.NewStaticCredentials(
	// 			cfg.AccessKey,
	// 			cfg.SecretKey,
	// 			"", // a token will be created when the session it's used.
	// 		),
	// 	})

	if err != nil {
		return nil, err
	}

	s3ses := s3.New(session.Must(s, err))
	if err != nil {
		return nil, err
	}
	return &S3Session{s3ses}, nil
}

func (s *S3Session) ListObjects(bucketName, folder string) ([]S3Object, error) {
	result, err := s.ListObjectsV2WithContext(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(folder),
	})
	if err != nil {
		//log.Printf("Couldn't list objects in bucket %v. Here's why: %v\n", bucketName, err)
		return nil, err
	}

	contents := make([]S3Object, 0, len(result.Contents)-1)
	for _, v := range result.Contents {
		contents = append(contents, S3Object{Filename: *v.Key, Etag: *v.ETag, Size: *v.Size, LastModified: *v.LastModified})
	}

	return contents, err
}

func (s *S3Session) GetFile(w http.ResponseWriter, bucket, filekey string) (int64, error) {
	result, err := s.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filekey),
	})
	if err != nil {
		//http.Error(w, fmt.Sprintf("Error getting file from s3 %s", err.Error()), http.StatusInternalServerError)
		return 0, fmt.Errorf("error getting file from s3 %s", err.Error())
	}

	//w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", "my-file.csv"))
	//w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", *result.ContentType)
	defer result.Body.Close()
	bytesWritten, copyErr := io.Copy(w, result.Body)
	if copyErr != nil {
		//http.Error(w, fmt.Errorf("Error copying file to the http response %s", copyErr.Error()), http.StatusInternalServerError)
		return 0, fmt.Errorf("error copying file to the http response %s", copyErr.Error())
	}

	return bytesWritten, nil
}

// UploadFile will upload a single file to S3, it will require a pre-built aws session
// and will set file info like content type and encryption on the uploaded file.
func (s *S3Session) UploadFile(fileToUpload, remoteFileName, bucket, contentType string) error {

	// Open the file for use
	file, err := os.Open(fileToUpload)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a bufferimage
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	if remoteFileName == "" {
		remoteFileName = filepath.Base(fileToUpload)
	}

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	if contentType == "" {
		contentType = *aws.String(http.DetectContentType(buffer))
	}
	_, err = s.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(remoteFileName),
		//ACL:                  aws.String("private"),
		Body:          bytes.NewReader(buffer),
		ContentLength: aws.Int64(size),
		ContentType:   &contentType,
		//ContentDisposition:   aws.String("attachment"),
		//ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// UploadLargeFile will upload a single large file to S3 as multipart uplaod, it will require a pre-built aws session
func (s *S3Session) UploadLargeFile(fileToUpload, remoteFileName, bucket, contentType string) error {
	// Open the file for use
	file, err := os.Open(fileToUpload)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size
	stats, _ := file.Stat()
	fileSize := stats.Size()

	// put file in byteArray
	buffer := make([]byte, fileSize)
	file.Read(buffer)

	if remoteFileName == "" {
		remoteFileName = filepath.Base(fileToUpload)
	}

	if contentType == "" {
		contentType = *aws.String(http.DetectContentType(buffer))
	}

	// Create MultipartUpload object
	//expiryDate := time.Now().AddDate(99, 0, 1)
	createdResp, err := s.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(remoteFileName),
		ContentType: &contentType,
		//Expires: &expiryDate,
	})

	if err != nil {
		fmt.Println(err)
		return err
	}

	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	var completedParts []*s3.CompletedPart
	// Loop till remaining upload size is 0
	for start = 0; remaining != 0; start += _PART_SIZE {
		if remaining < _PART_SIZE {
			currentSize = remaining
		} else {
			currentSize = _PART_SIZE
		}

		completed, err := s.multipartUpload(createdResp, buffer[start:start+currentSize], partNum)
		// If upload function failed (meaning it retried acoording to RETRIES)
		if err != nil {
			_, err = s.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   createdResp.Bucket,
				Key:      createdResp.Key,
				UploadId: createdResp.UploadId,
			})
			if err != nil {
				// god speed
				fmt.Println(err)
				return err
			}
		}

		// Detract the current part size from remaining
		remaining -= currentSize
		fmt.Printf("Part %v complete, %v btyes remaining\n", partNum, remaining)

		// Add the completed part to our list
		completedParts = append(completedParts, completed)
		partNum++

	}

	// All the parts are uploaded, completing the upload
	resp, err := s.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp.String())
	}

	return nil
}

// RemoveFile will remove given remote-file from S3 storage
func (s *S3Session) RemoveFile(bucket, remoteFileName string) error {
	_, err := s.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(remoteFileName)})
	if err != nil {
		return err
	}

	err = s.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(remoteFileName),
	})
	return err
}

// RemoveFolder will remove given folder and all of it's files from S3
func (s *S3Session) RemoveFolder(bucket, folder string) error {
	result, err := s.ListObjectsV2WithContext(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(folder),
	})
	//var contents []types.Object
	if err != nil {
		//log.Printf("Couldn't list objects in bucket %v. Here's why: %v\n", bucket, err)
		return err
	}

	keys := []*s3.ObjectIdentifier{}
	for _, v := range result.Contents {
		keys = append(keys, &s3.ObjectIdentifier{Key: v.Key})
	}

	_, err = s.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: keys,
			Quiet:   aws.Bool(false),
		},
	})
	if err != nil {
		return err
	}

	// delete folder itself
	_, err = s.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(folder)})
	return err
}
