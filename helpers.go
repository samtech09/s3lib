package s3lib

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Uploads the fileBytes bytearray a MultiPart upload
func (s *S3Session) multipartUpload(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int) (completedPart *s3.CompletedPart, err error) {
	var try int
	for try <= _RETRIES {
		uploadResp, err := s.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		// Upload failed
		if err != nil {
			s.logentry("multipartUpload", "[partnum:%d, try:%d] multipartUpload error: %s", partNum, try, err.Error())
			// Max retries reached! Quitting
			if try == _RETRIES {
				return nil, err
			} else {
				// Retrying
				try++
			}
		} else {
			// Upload is done!
			return &s3.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int64(int64(partNum)),
			}, nil
		}
	}

	return nil, nil
}

func (s *S3Session) removeBulkGcs(bucket, folder string, list *s3.ListObjectsV2Output) error {
	var err error
	for _, v := range list.Contents {
		_, err = s.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(*v.Key)})
		if err != nil {
			s.logentry("removeBulkGcs", "[bucket:%s, key:%s] delete error: %s", bucket, *v.Key, err.Error())
			return fmt.Errorf("object delete error: %s", err.Error())
		}
	}
	s.logentry("removeBulkGcs", "[bucket:%s, folder:%s] all objects deleted", bucket, folder)

	//in gcs empty folder will be deleted itself

	return nil
}
