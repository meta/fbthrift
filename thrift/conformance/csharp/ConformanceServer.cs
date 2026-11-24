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

#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using FBThrift;
using Apache.Thrift.Conformance;
using Apache.Thrift.Conformance.Protocol;
using Apache.Thrift.Conformance.Serialization;
using AnyNs = Apache.Thrift.Conformance.Any;

namespace FBThrift.Conformance.Server
{
    /// <summary>
    /// Minimal Thrift conformance server for C#.
    /// Listens on a dynamic port, prints the port to stdout, and handles
    /// ConformanceService.roundTrip requests using the Thrift binary message envelope.
    /// Note: The Header protocol is deprecated, but is supported here because the
    /// conformance test harness sends requests using it.
    /// </summary>
    public static class ConformanceServer
    {
        private static readonly Dictionary<string, Func<IThriftSerializable>> TypeRegistryByName
            = new Dictionary<string, Func<IThriftSerializable>>();

        private static readonly Dictionary<string, Func<IThriftSerializable>> TypeRegistryByHash
            = new Dictionary<string, Func<IThriftSerializable>>();

        private static volatile bool _running = true;

        public static void Main(string[] args)
        {
            RegisterAllTypes();

            var listener = new TcpListener(IPAddress.IPv6Loopback, 0);
            listener.Server.DualMode = true;
            listener.Start();

            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            Console.WriteLine(port);
            Console.Out.Flush();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                _running = false;
                listener.Stop();
            };

            try
            {
                while (_running)
                {
                    var client = listener.AcceptTcpClient();
                    ThreadPool.QueueUserWorkItem(_ => HandleClient(client));
                }
            }
            catch (SocketException) when (!_running)
            {
                // Expected when listener is stopped
            }
        }

        private static void HandleClient(TcpClient client)
        {
            try
            {
                using (client)
                using (var stream = client.GetStream())
                {
                    while (_running && client.Connected)
                    {
                        try
                        {
                            ProcessRequest(stream);
                        }
                        catch (EndOfStreamException)
                        {
                            break;
                        }
                        catch (IOException)
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Client handler error: {ex.Message}");
            }
        }

        private static void ProcessRequest(NetworkStream stream)
        {
            // Read framed message: 4-byte big-endian length prefix
            var lenBuf = new byte[4];
            ReadExact(stream, lenBuf, 4);
            int frameLen = (lenBuf[0] << 24) | (lenBuf[1] << 16) | (lenBuf[2] << 8) | lenBuf[3];

            if (frameLen <= 0 || frameLen > 16 * 1024 * 1024)
            {
                throw new ThriftProtocolException($"Invalid frame length: {frameLen}");
            }

            var frameData = new byte[frameLen];
            ReadExact(stream, frameData, frameLen);

            // Detect Header protocol: magic bytes 0x0FFF at start
            bool isHeaderProtocol = frameLen >= 10 && frameData[0] == 0x0F && frameData[1] == 0xFF;

            int headerSeqId = 0;
            int headerProtoId = 0; // 0 = Binary, 2 = Compact
            byte[] payloadData;

            if (isHeaderProtocol)
            {
                // Header protocol envelope:
                // bytes 0-1: magic (0x0FFF)
                // bytes 2-3: flags
                // bytes 4-7: sequence ID (big-endian)
                // bytes 8-9: header size in 32-bit words (big-endian)
                // then header_size*4 bytes of header metadata:
                //   - protocol ID (varint)
                //   - number of transforms (varint)
                //   - transform IDs (each varint)
                //   - info headers (key-value pairs)
                // then the Binary/Compact protocol payload
                headerSeqId = (frameData[4] << 24) | (frameData[5] << 16) | (frameData[6] << 8) | frameData[7];
                int headerSizeWords = (frameData[8] << 8) | frameData[9];
                int headerSizeBytes = headerSizeWords * 4;

                // Parse header metadata to extract protocol ID
                if (headerSizeBytes > 0)
                {
                    using var headerStream = new MemoryStream(frameData, 10, headerSizeBytes);
                    headerProtoId = ReadVarint32(headerStream);
                    // Skip transforms (read count, then skip each)
                    int numTransforms = ReadVarint32(headerStream);
                    for (int i = 0; i < numTransforms; i++)
                    {
                        ReadVarint32(headerStream); // transform ID
                    }
                }

                int payloadOffset = 10 + headerSizeBytes;
                int payloadLen = frameLen - payloadOffset;
                payloadData = new byte[payloadLen];
                Array.Copy(frameData, payloadOffset, payloadData, 0, payloadLen);
            }
            else
            {
                payloadData = frameData;
            }

            using var requestStream = new MemoryStream(payloadData);

            if (payloadData.Length == 0)
            {
                throw new ThriftProtocolException("Empty payload after header parsing");
            }

            // Detect protocol: Binary (0x80) or Compact (0x82)
            byte firstByte = payloadData[0];
            string methodName;
            int seqId;

            if (firstByte == 0x82)
            {
                // Compact Protocol message envelope:
                // byte 0: protocol id (0x82)
                // byte 1: version (high 5 bits) + message type (low 3 bits)
                // varint: sequence id
                // string: method name (varint length + utf8 bytes)
                requestStream.ReadByte(); // skip protocol id (0x82)
                int versionAndType2 = requestStream.ReadByte();
                // Read sequence id as varint
                seqId = ReadVarint32(requestStream);
                // Read method name
                int nameLen = ReadVarint32(requestStream);
                var nameBytes = new byte[nameLen];
                ReadExact(requestStream, nameBytes, nameLen);
                methodName = Encoding.UTF8.GetString(nameBytes);
            }
            else
            {
                // Binary Protocol message envelope
                var requestReader2 = new ThriftBinaryReader(requestStream);
                int versionAndType2 = requestReader2.ReadI32();
                int version = versionAndType2 & unchecked((int)0xFFFF0000);
                if (version != unchecked((int)0x80010000))
                {
                    throw new ThriftProtocolException($"Bad version in message envelope: 0x{version:X8}");
                }
                methodName = requestReader2.ReadString();
                seqId = requestReader2.ReadI32();
            }

            // For Header protocol, use the header's sequence ID
            if (isHeaderProtocol)
            {
                seqId = headerSeqId;
            }

            // Create a reader from the remaining stream position (after envelope)
            bool useCompactEnvelope = (firstByte == 0x82);
            IThriftProtocolReader requestReader;
            if (useCompactEnvelope)
            {
                requestReader = new ThriftCompactReader(requestStream);
            }
            else
            {
                requestReader = new ThriftBinaryReader(requestStream);
            }

            // Process the method call
            using var responseBody = new MemoryStream();

            if (methodName == "roundTrip")
            {
                HandleRoundTripGeneric(requestReader, responseBody, seqId, useCompactEnvelope);
            }
            else
            {
                WriteGenericApplicationException(responseBody, methodName, seqId, useCompactEnvelope, $"Unknown method: {methodName}");
            }

            // Build response
            var responsePayload = responseBody.ToArray();

            if (isHeaderProtocol)
            {
                // Build header metadata: protocol ID (varint) + transform count (varint, 0)
                using var headerMeta = new MemoryStream();
                WriteVarint32(headerMeta, headerProtoId);
                WriteVarint32(headerMeta, 0); // no transforms
                var headerMetaBytes = headerMeta.ToArray();
                // Header size is in 32-bit words, rounded up
                int headerSizeWords = (headerMetaBytes.Length + 3) / 4;
                int headerSizeBytes = headerSizeWords * 4;
                // Pad header metadata to align to 4 bytes
                var paddedHeaderMeta = new byte[headerSizeBytes];
                Array.Copy(headerMetaBytes, paddedHeaderMeta, headerMetaBytes.Length);

                // Wrap in Header protocol envelope:
                // magic(2) + flags(2) + seqId(4) + headerSize(2) + headerMeta + payload
                int headerFrameLen = 10 + headerSizeBytes + responsePayload.Length;
                var responseFrame = new byte[4 + headerFrameLen];
                // Frame length prefix
                responseFrame[0] = (byte)((headerFrameLen >> 24) & 0xFF);
                responseFrame[1] = (byte)((headerFrameLen >> 16) & 0xFF);
                responseFrame[2] = (byte)((headerFrameLen >> 8) & 0xFF);
                responseFrame[3] = (byte)(headerFrameLen & 0xFF);
                // Magic
                responseFrame[4] = 0x0F;
                responseFrame[5] = 0xFF;
                // Flags
                responseFrame[6] = 0x00;
                responseFrame[7] = 0x00;
                // Sequence ID
                responseFrame[8] = (byte)((headerSeqId >> 24) & 0xFF);
                responseFrame[9] = (byte)((headerSeqId >> 16) & 0xFF);
                responseFrame[10] = (byte)((headerSeqId >> 8) & 0xFF);
                responseFrame[11] = (byte)(headerSeqId & 0xFF);
                // Header size in words
                responseFrame[12] = (byte)((headerSizeWords >> 8) & 0xFF);
                responseFrame[13] = (byte)(headerSizeWords & 0xFF);
                // Header metadata (protocol ID + transform count + padding)
                Array.Copy(paddedHeaderMeta, 0, responseFrame, 14, headerSizeBytes);
                // Payload
                Array.Copy(responsePayload, 0, responseFrame, 14 + headerSizeBytes, responsePayload.Length);
                stream.Write(responseFrame, 0, responseFrame.Length);
            }
            else
            {
                // Raw framed Binary protocol response
                var responseLenBuf = new byte[4];
                responseLenBuf[0] = (byte)((responsePayload.Length >> 24) & 0xFF);
                responseLenBuf[1] = (byte)((responsePayload.Length >> 16) & 0xFF);
                responseLenBuf[2] = (byte)((responsePayload.Length >> 8) & 0xFF);
                responseLenBuf[3] = (byte)(responsePayload.Length & 0xFF);
                stream.Write(responseLenBuf, 0, 4);
                stream.Write(responsePayload, 0, responsePayload.Length);
            }
            stream.Flush();
        }

        private static void HandleRoundTripGeneric(IThriftProtocolReader requestReader, Stream responseBody, int seqId, bool useCompact)
        {
            // Read the args struct (field 1 = RoundTripRequest)
            var request = new @RoundTripRequest();
            while (true)
            {
                var (ft, fid) = requestReader.ReadFieldBegin();
                if (ft == ThriftWireType.Stop) break;
                if (fid == 1 && ft == ThriftWireType.Struct)
                {
                    request = requestReader.ReadStruct<@RoundTripRequest>();
                }
                else
                {
                    requestReader.Skip(ft);
                }
            }

            // Perform round-trip
            @RoundTripResponse response;
            try
            {
                response = DoRoundTrip(request);
            }
            catch (Exception ex)
            {
                WriteGenericApplicationException(responseBody, "roundTrip", seqId, useCompact, ex.Message);
                return;
            }

            if (useCompact)
            {
                // Compact protocol reply envelope:
                // byte 0: protocol id (0x82)
                // byte 1: (type << 5) | version  -- REPLY=2, version=2 => 0x42
                // varint: sequence id
                // string: method name
                responseBody.WriteByte(0x82);
                responseBody.WriteByte((2 << 5) | 2); // 0x42: type=REPLY(2), version=2
                WriteVarint32(responseBody, seqId);
                var nameBytes = Encoding.UTF8.GetBytes("roundTrip");
                WriteVarint32(responseBody, nameBytes.Length);
                responseBody.Write(nameBytes, 0, nameBytes.Length);

                var writer = new ThriftCompactWriter(responseBody);
                writer.WriteFieldBegin(ThriftWireType.Struct, 0);
                writer.WriteStruct(response);
                writer.WriteFieldStop();
            }
            else
            {
                var writer = new ThriftBinaryWriter(responseBody);
                WriteMessageBegin(writer, "roundTrip", 2 /* REPLY */, seqId);
                writer.WriteFieldBegin(ThriftWireType.Struct, 0);
                writer.WriteStruct(response);
                writer.WriteFieldStop();
            }
        }

        private static void WriteGenericApplicationException(Stream responseBody, string methodName, int seqId, bool useCompact, string message)
        {
            if (useCompact)
            {
                // Compact protocol exception envelope
                responseBody.WriteByte(0x82); // protocol id
                responseBody.WriteByte((3 << 5) | 2); // 0x62: type=EXCEPTION(3), version=2
                WriteVarint32(responseBody, seqId);
                var nameBytes = Encoding.UTF8.GetBytes(methodName);
                WriteVarint32(responseBody, nameBytes.Length);
                responseBody.Write(nameBytes, 0, nameBytes.Length);

                var writer = new ThriftCompactWriter(responseBody);
                writer.WriteFieldBegin(ThriftWireType.String, 1);
                writer.WriteString(message);
                writer.WriteFieldBegin(ThriftWireType.I32, 2);
                writer.WriteI32(7); // INTERNAL_ERROR
                writer.WriteFieldStop();
            }
            else
            {
                var writer = new ThriftBinaryWriter(responseBody);
                WriteApplicationException(writer, methodName, seqId, message);
            }
        }

        private static @RoundTripResponse DoRoundTrip(@RoundTripRequest request)
        {
            var any = request.@value;
            if (any == null)
            {
                throw new Exception("RoundTripRequest.value is null");
            }

            // Determine source protocol
            // Note: Compact is the default protocol when not specified (matching C++ getProtocol behavior)
            var sourceProtocol = any.@protocol ?? StandardProtocol.Compact;

            // Determine target protocol
            StandardProtocol targetProtocol;
            if (request.@targetProtocol != null)
            {
                targetProtocol = request.@targetProtocol.@standard;
            }
            else
            {
                targetProtocol = sourceProtocol;
            }

            // Look up the type
            IThriftSerializable obj = LoadType(any);

            // Deserialize from source protocol
            var data = any.@data ?? Array.Empty<byte>();
            using var readStream = new MemoryStream(data);
            IThriftProtocolReader reader = CreateReader(sourceProtocol, readStream);
            obj.__fbthrift_read(reader);

            // Re-serialize with target protocol
            byte[] encodedData;
            switch (targetProtocol)
            {
                case StandardProtocol.Binary:
                    encodedData = ThriftSerializer.EncodeBinary(obj);
                    break;
                case StandardProtocol.Compact:
                case StandardProtocol.Custom:
                    // Custom falls through to Compact: the C++ conformance server's
                    // getProtocol() returns Compact for both Compact and Custom, so we
                    // match that behavior here.
                    encodedData = ThriftSerializer.EncodeCompact(obj);
                    break;
                default:
                    throw new Exception($"Unsupported target protocol: {targetProtocol}");
            }

            var responseAny = new AnyNs.@Any
            {
                @type = any.@type,
                @typeHashPrefixSha2_256 = any.@typeHashPrefixSha2_256,
                @protocol = targetProtocol,
                @customProtocol = any.@customProtocol,
                @data = encodedData
            };

            return new @RoundTripResponse { @value = responseAny };
        }

        private static IThriftSerializable LoadType(AnyNs.@Any any)
        {
            if (any.@type != null)
            {
                if (TypeRegistryByName.TryGetValue(any.@type, out var factory))
                {
                    return factory();
                }
                throw new Exception($"Unknown type URI: {any.@type}");
            }

            if (any.@typeHashPrefixSha2_256 != null && any.@typeHashPrefixSha2_256.Length > 0)
            {
                var hexHash = BitConverter.ToString(any.@typeHashPrefixSha2_256).Replace("-", "").ToLowerInvariant();
                // Try matching by prefix
                foreach (var kvp in TypeRegistryByHash)
                {
                    if (kvp.Key.StartsWith(hexHash) || hexHash.StartsWith(kvp.Key))
                    {
                        return kvp.Value();
                    }
                }
                throw new Exception($"Unknown type hash: {hexHash}");
            }

            throw new Exception("Any has neither type nor typeHashPrefixSha2_256 set");
        }

        private static IThriftProtocolReader CreateReader(StandardProtocol protocol, Stream stream)
        {
            switch (protocol)
            {
                case StandardProtocol.Binary:
                    return new ThriftBinaryReader(stream);
                case StandardProtocol.Compact:
                case StandardProtocol.Custom:
                    return new ThriftCompactReader(stream);
                default:
                    throw new Exception($"Unsupported source protocol: {protocol}");
            }
        }

        private static void WriteMessageBegin(ThriftBinaryWriter writer, string name, byte type, int seqId)
        {
            int versionAndType = unchecked((int)0x80010000) | type;
            writer.WriteI32(versionAndType);
            writer.WriteString(name);
            writer.WriteI32(seqId);
        }

        private static void WriteApplicationException(ThriftBinaryWriter writer, string methodName, int seqId, string message)
        {
            // Write EXCEPTION envelope (type = 3)
            WriteMessageBegin(writer, methodName, 3 /* EXCEPTION */, seqId);
            // TApplicationException struct: field 1 = message (string), field 2 = type (i32)
            writer.WriteFieldBegin(ThriftWireType.String, 1);
            writer.WriteString(message);
            writer.WriteFieldBegin(ThriftWireType.I32, 2);
            writer.WriteI32(7); // INTERNAL_ERROR
            writer.WriteFieldStop();
        }

        private static void RegisterAllTypes()
        {
            // Explicitly register all types from each generated thrift module.
            // Each module's {module}TypeRegistry.RegisterTypes() is generated by the
            // thrift compiler (matching the Go/Rust conformance server pattern).
            // Only modules with structured types that have thrift_uri are listed.
            // Class names are derived from the .thrift filename (e.g., testset.thrift
            // generates testsetTypeRegistry).
            Apache.Thrift.Test.Testset.@testsetTypeRegistry.RegisterTypes(RegisterType);
            Apache.Thrift.Test.Enum.@EnumTypeRegistry.RegisterTypes(RegisterType);
            Apache.Thrift.Test.Terse.@TerseTypeRegistry.RegisterTypes(RegisterType);
            facebook.thrift.protocol.detail.@protocol_detailTypeRegistry.RegisterTypes(RegisterType);
        }

        private static void RegisterType(string thriftUri, Func<IThriftSerializable> factory)
        {
            TypeRegistryByName[thriftUri] = factory;

            using var sha256 = SHA256.Create();
            var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes("fbthrift://" + thriftUri));
            var hexHash = BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            TypeRegistryByHash[hexHash] = factory;
        }

        private static void ReadExact(Stream stream, byte[] buffer, int count)
        {
            int offset = 0;
            while (offset < count)
            {
                int read = stream.Read(buffer, offset, count - offset);
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }
                offset += read;
            }
        }

        private static int ReadVarint32(Stream stream)
        {
            int result = 0;
            int shift = 0;
            while (true)
            {
                int b = stream.ReadByte();
                if (b < 0) throw new EndOfStreamException();
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            return result;
        }

        private static void WriteVarint32(Stream stream, int value)
        {
            uint uvalue = (uint)value;
            while (uvalue >= 0x80)
            {
                stream.WriteByte((byte)(uvalue | 0x80));
                uvalue >>= 7;
            }
            stream.WriteByte((byte)uvalue);
        }
    }
}
