package r2

import (
	"testing"

	"github.com/aws/smithy-go"
)

func TestIsNotFoundDistinguishesMissingKeyFromMissingBucket(t *testing.T) {
	if !IsNotFound(&smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}) {
		t.Fatal("NoSuchKey should be not found")
	}
	if IsNotFound(&smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}) {
		t.Fatal("NoSuchBucket should remain an error")
	}
}
