/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thrift

import (
	"errors"
	"fmt"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift/format"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift/types"
	"github.com/facebook/fbthrift/thrift/lib/thrift/rpcmetadata"
	"github.com/rsocket/rsocket-go/core"
)

type unknownMethodError struct {
	message string
	data    []byte
}

var _ core.CustomError = (*unknownMethodError)(nil)

func newUnknownMethodError(method string) error {
	message := fmt.Sprintf("Method name %s not found", method)
	rpcErr := rpcmetadata.NewResponseRpcError().
		SetNameUTF8(new("UNKNOWN_METHOD")).
		SetWhatUTF8(&message).
		SetCategory(new(rpcmetadata.ResponseRpcErrorCategory_INVALID_REQUEST)).
		SetCode(new(rpcmetadata.ResponseRpcErrorCode_UNKNOWN_METHOD))
	data, err := format.EncodeCompact(rpcErr)
	if err != nil {
		return fmt.Errorf("encode unknown method error: %w", err)
	}
	return &unknownMethodError{message: message, data: data}
}

func (e *unknownMethodError) Error() string { return e.message }

func (e *unknownMethodError) ErrorCode() core.ErrorCode { return core.ErrorCodeInvalid }

func (e *unknownMethodError) ErrorData() []byte { return e.data }

// decodeResponseError maps supported structured Rocket refusals to Thrift errors.
// Unstructured application errors and unsupported error codes remain unchanged.
func decodeResponseError(err error) error {
	frame, ok := errors.AsType[core.CustomError](err)
	if !ok || frame.ErrorCode() != core.ErrorCodeInvalid {
		return err
	}
	rpcErr := rpcmetadata.NewResponseRpcError()
	if DecodeCompact(frame.ErrorData(), rpcErr) != nil ||
		!rpcErr.IsSetCode() || rpcErr.GetCode() != rpcmetadata.ResponseRpcErrorCode_UNKNOWN_METHOD {
		return err
	}
	return types.NewApplicationException(types.UNKNOWN_METHOD, rpcErr.GetWhatUTF8())
}
