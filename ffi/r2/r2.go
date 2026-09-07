// Package r2 bridges S3 error classification that is not ergonomic in Ard.
package r2

import (
	"errors"

	"github.com/aws/smithy-go"
)

// IsNotFound reports whether an S3 error conclusively identifies a missing object.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiError smithy.APIError
	return errors.As(err, &apiError) && (apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound")
}
