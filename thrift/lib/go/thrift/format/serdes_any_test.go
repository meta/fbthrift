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

package format

import (
	"testing"

	thriftstandard "thrift/lib/thrift/standard"

	dummyif "github.com/facebook/fbthrift/thrift/test/go/if/dummy"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeAnyCompact(t *testing.T) {
	original := dummyif.NewDummyStruct1().
		SetField1(int8(42)).
		SetField2(true).
		SetField3(int16(12345)).
		SetField4(int32(123456)).
		SetField5(int64(1234567890)).
		SetField6(float32(3.14)).
		SetField7(float64(2.71828)).
		SetField8([]byte{0xDE, 0xAD, 0xBE, 0xEF}).
		SetField9("hello thrift any")

	// Encode struct to Any
	anyStruct, err := EncodeStructToAny(original, thriftstandard.StandardProtocol_Compact)
	require.NoError(t, err)
	require.NotNil(t, anyStruct)

	// Decode Any back to struct
	decoded, err := DecodeAny(anyStruct)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	// Assert equality
	decodedStruct, ok := decoded.(*dummyif.DummyStruct1)
	require.True(t, ok, "decoded value should be *DummyStruct1")
	require.Equal(t, original.GetField1(), decodedStruct.GetField1())
	require.Equal(t, original.GetField2(), decodedStruct.GetField2())
	require.Equal(t, original.GetField3(), decodedStruct.GetField3())
	require.Equal(t, original.GetField4(), decodedStruct.GetField4())
	require.Equal(t, original.GetField5(), decodedStruct.GetField5())
	require.Equal(t, original.GetField6(), decodedStruct.GetField6())
	require.Equal(t, original.GetField7(), decodedStruct.GetField7())
	require.Equal(t, original.GetField8(), decodedStruct.GetField8())
	require.Equal(t, original.GetField9(), decodedStruct.GetField9())
}

func TestEncodeDecodeAnyBinary(t *testing.T) {
	original := dummyif.NewDummyStruct1().
		SetField1(int8(-128)).
		SetField2(false).
		SetField3(int16(-32768)).
		SetField4(int32(-2147483648)).
		SetField5(int64(-9223372036854775808)).
		SetField6(float32(-1.5)).
		SetField7(float64(-2.5)).
		SetField8([]byte{}).
		SetField9("")

	// Encode struct to Any
	anyStruct, err := EncodeStructToAny(original, thriftstandard.StandardProtocol_Binary)
	require.NoError(t, err)
	require.NotNil(t, anyStruct)

	// Decode Any back to struct
	decoded, err := DecodeAny(anyStruct)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	// Assert equality
	decodedStruct, ok := decoded.(*dummyif.DummyStruct1)
	require.True(t, ok, "decoded value should be *DummyStruct1")
	require.Equal(t, original.GetField1(), decodedStruct.GetField1())
	require.Equal(t, original.GetField2(), decodedStruct.GetField2())
	require.Equal(t, original.GetField3(), decodedStruct.GetField3())
	require.Equal(t, original.GetField4(), decodedStruct.GetField4())
	require.Equal(t, original.GetField5(), decodedStruct.GetField5())
	require.Equal(t, original.GetField6(), decodedStruct.GetField6())
	require.Equal(t, original.GetField7(), decodedStruct.GetField7())
	require.Equal(t, original.GetField8(), decodedStruct.GetField8())
	require.Equal(t, original.GetField9(), decodedStruct.GetField9())
}

func TestEncodeStructToAnyNilValue(t *testing.T) {
	_, err := EncodeStructToAny(nil, thriftstandard.StandardProtocol_Compact)
	require.Error(t, err)
	require.Contains(t, err.Error(), "value is nil")
}
