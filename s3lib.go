package s3lib

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
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
	Gcs              bool // google-cloud-storage
	Debug            bool
}

type S3Object struct {
	Filename     string
	Etag         string
	LastModified time.Time
	Size         int64
}

type S3Session struct {
	*s3.S3
	Debug bool
	Gcs   bool
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
	return &S3Session{s3ses, cfg.Debug, cfg.Gcs}, nil
}

func (s *S3Session) ListObjects(bucket, folder string) ([]S3Object, error) {
	result, err := s.ListObjectsV2WithContext(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(folder),
	})
	if err != nil {
		s.logentry("ListObjects", "[bucket:%s] list objects error: %s", bucket, err.Error())
		return nil, err
	}

	contents := make([]S3Object, 0, len(result.Contents)-1)
	for _, v := range result.Contents {
		contents = append(contents, S3Object{Filename: *v.Key, Etag: *v.ETag, Size: *v.Size, LastModified: *v.LastModified})
	}

	s.logentry("ListObjects", "[bucket:%s] %d objects listed", bucket, len(contents))
	return contents, nil
}

func (s *S3Session) GetFile(w http.ResponseWriter, bucket, filekey string, cacheDurinSeconds int) (int64, error) {
	result, err := s.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filekey),
	})
	if err != nil {
		s.logentry("GetFile", "[bucket:%s, key:%s] getobject error: %s", bucket, filekey, err.Error())
		return 0, fmt.Errorf("error getting file from storage %s", err.Error())
	}

	//w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", "my-file.csv"))
	if cacheDurinSeconds > 0 {
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", cacheDurinSeconds)) // cache for 6 months
		w.Header().Set("Etag", s.Hash(filekey, result.LastModified.String()))
	}

	w.Header().Set("Content-Type", *result.ContentType)
	defer result.Body.Close()
	bytesWritten, copyErr := io.Copy(w, result.Body)
	if copyErr != nil {
		s.logentry("GetFile", "[bucket:%s, key:%s] error copying file to http response: %s", bucket, filekey, copyErr.Error())
		return 0, fmt.Errorf("error copying file to the http response %s", copyErr.Error())
	}

	s.logentry("ListObjects", "[bucket:%s, key:%s] getobject succeeded", bucket, filekey)
	return bytesWritten, nil
}

// HeadFile make head request to S3 storage and get metadata of file without getting file contents
func (s *S3Session) HeadFile(bucket, filekey string) (time.Time, error) {
	result, err := s.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filekey),
	})
	if err != nil {
		s.logentry("HeadFile", "[bucket:%s, key:%s] headobject error: %s", bucket, filekey, err.Error())
		return time.Now(), fmt.Errorf("error heading file from storage %s", err.Error())
	}
	s.logentry("HeadFile", "[bucket:%s, key:%s] headobject succeeded", bucket, filekey)
	return *result.LastModified, nil
}

// UploadFile will upload a single file to S3, it will require a pre-built aws session
// and will set file info like content type and encryption on the uploaded file.
func (s *S3Session) UploadFile(fileToUpload, remoteFileName, bucket, contentType string) error {
	// Open the file for use
	file, err := os.Open(fileToUpload)
	if err != nil {
		s.logentry("UploadFile", "[local-file:%s] file open error: %s", fileToUpload, err.Error())
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
	if err != nil {
		s.logentry("UploadFile", "[bucket:%s, local-file:%s] putobject error: %s", bucket, fileToUpload, err.Error())
		return err
	}
	s.logentry("UploadFile", "[bucket:%s, local-file:%s] putobject succeeded", bucket, fileToUpload)
	return nil
}

// UploadLargeFile will upload a single large file to S3 as multipart uplaod, it will require a pre-built aws session
func (s *S3Session) UploadLargeFile(fileToUpload, remoteFileName, bucket, contentType string) error {
	// Open the file for use
	file, err := os.Open(fileToUpload)
	if err != nil {
		s.logentry("UploadLargeFile", "[local-file:%s] file open error: %s", fileToUpload, err.Error())
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
		s.logentry("UploadLargeFile", "[bucket:%s, local-file:%s, contentType:%s] CreateMultipartUpload error: %s", bucket, fileToUpload, contentType, err.Error())
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
				s.logentry("UploadLargeFile", "[bucket:%s, local-file:%s, contentType:%s] multipartUpload error: %s", bucket, fileToUpload, contentType, err.Error())
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
		s.logentry("UploadLargeFile", "[bucket:%s, local-file:%s, contentType:%s] CompleteMultipartUpload error: %s", bucket, fileToUpload, contentType, err.Error())
	} else {
		s.logentry("UploadLargeFile", "[bucket:%s, local-file:%s, contentType:%s] upload-large-file succeeded", bucket, fileToUpload, contentType)
		fmt.Println(resp.String())
	}

	return nil
}

// RemoveFile will remove given remote-file from S3 storage
func (s *S3Session) RemoveFile(bucket, remoteFileName string) error {
	_, err := s.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(remoteFileName)})
	if err != nil {
		s.logentry("RemoveFile", "[bucket:%s, key:%s] delete error: %s", bucket, remoteFileName, err.Error())
		return err
	}

	err = s.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(remoteFileName),
	})
	if err != nil {
		s.logentry("RemoveFile", "[bucket:%s, key:%s] WaitUntilObjectNotExists error: %s", bucket, remoteFileName, err.Error())
		return err
	}
	s.logentry("RemoveFile", "[bucket:%s, key:%s] delete succeeded", bucket, remoteFileName)
	return nil
}

// RemoveFolder will remove given folder and all of it's files from S3
func (s *S3Session) RemoveFolder(bucket, folder string) error {
	result, err := s.ListObjectsV2WithContext(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(folder),
	})
	//var contents []types.Object
	if err != nil {
		s.logentry("RemoveFolder", "[bucket:%s] list objects error: %s", bucket, err.Error())
		return fmt.Errorf("list objects error: %s", err.Error())
	}

	if s.Gcs {
		s.removeBulkGcs(bucket, folder, result)
	} else {
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
			s.logentry("RemoveFolder", "[keys: %d] buld delete error: %s", len(keys), err.Error())
			return fmt.Errorf("bulk delete error: %s", err.Error())
		}
		s.logentry("RemoveFolder", "[bucket:%s, folder:%s] all objects deleted", bucket, folder)

		// delete folder itself
		_, err = s.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(folder)})
		if err != nil {
			s.logentry("RemoveFolder", "[bucket:%s, key:%s] folder delete error: %s", bucket, folder, err.Error())
			return fmt.Errorf("object delete error: %s", err.Error())
		}
	}

	s.logentry("RemoveFolder", "[bucket:%s, key:%s] folder delete succeeded", bucket, folder)
	return nil
}

// md5Str gives MD5 hash of given string and salt
func (s *S3Session) Hash(filekey, lastmodified string) string {
	h := md5.New()
	h.Write([]byte(filekey + lastmodified))

	return fmt.Sprintf("%x", h.Sum(nil))
}

func (s *S3Session) logentry(method, format string, v ...any) {
	if s.Debug {
		// add method to beginning of passed arguments for formating
		v = addElementToFirstIndex(v, method)
		log.Printf("[%s] "+format, v...)
	}
}

func addElementToFirstIndex(x []interface{}, y interface{}) []interface{} {
	x = append([]interface{}{y}, x...)
	return x
}
