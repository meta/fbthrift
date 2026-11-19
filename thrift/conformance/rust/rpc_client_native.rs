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

//! `rpc_client.rs` run over `nativeclient` instead of `bareclient`.
//!
//! Whatever `nativeclient` does not implement yet fails through `Transport`'s
//! default methods and is listed in
//! `thrift/conformance/data/nonconforming.txt`. Those entries are inverted
//! expectations and implementing one of those features turns the corresponding
//! test red until its line is deleted.
//!
//! The two files cannot stay identical. `bareclient` never implemented
//! `Transport::call_bidirectional`, so the `rust` category is listed as
//! nonconforming for every bidi case and `rpc_client.rs` still answers
//! them with an error. And `bareclient` gives a Rust caller no way to read an
//! exception sent inside a Thrift `Any`, so for the any-exception case
//! `rpc_client.rs` reports the message alone and is listed as nonconforming
//! too.
//!
//! This file should check exactly what `RPCClient.cpp` checks even though
//! `rpc_client.rs` is more relaxed and e.g. checks an error via text rather
//! than by it's type.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use clap::Parser;
use futures::FutureExt;
use futures::stream::StreamExt;
use nativeclient::ThriftErrorExt;
use rpc::rpc::BidiAbortClientInstruction;
use rpc::rpc::BidiAbortClientTestResult;
use rpc::rpc::BidiBasicClientInstruction;
use rpc::rpc::BidiBasicClientTestResult;
use rpc::rpc::BidiInitialResponseClientInstruction;
use rpc::rpc::BidiInitialResponseClientTestResult;
use rpc::rpc::BidiMethodDeclaredExceptionClientInstruction;
use rpc::rpc::BidiMethodDeclaredExceptionClientTestResult;
use rpc::rpc::BidiMethodUndeclaredExceptionClientInstruction;
use rpc::rpc::BidiMethodUndeclaredExceptionClientTestResult;
use rpc::rpc::BidiServerErrorClientInstruction;
use rpc::rpc::BidiServerErrorClientTestResult;
use rpc::rpc::BidiSinkDeclaredExceptionClientInstruction;
use rpc::rpc::BidiSinkDeclaredExceptionClientTestResult;
use rpc::rpc::BidiSinkUndeclaredExceptionClientInstruction;
use rpc::rpc::BidiSinkUndeclaredExceptionClientTestResult;
use rpc::rpc::BidiStreamDeclaredExceptionClientInstruction;
use rpc::rpc::BidiStreamDeclaredExceptionClientTestResult;
use rpc::rpc::BidiStreamUndeclaredExceptionClientInstruction;
use rpc::rpc::BidiStreamUndeclaredExceptionClientTestResult;
use rpc::rpc::SinkAbortClientInstruction;
use rpc::rpc::SinkAbortClientTestResult;
use rpc::rpc::SinkBasicClientInstruction;
use rpc::rpc::SinkBasicClientTestResult;
use rpc::rpc::SinkChunkTimeoutClientInstruction;
use rpc::rpc::SinkChunkTimeoutClientTestResult;
use rpc::rpc::SinkDeclaredExceptionClientInstruction;
use rpc::rpc::SinkDeclaredExceptionClientTestResult;
use rpc::rpc::SinkInitialDeclaredExceptionClientInstruction;
use rpc::rpc::SinkInitialDeclaredExceptionClientTestResult;
use rpc::rpc::SinkInitialResponseClientInstruction;
use rpc::rpc::SinkInitialResponseClientTestResult;
use rpc::rpc::SinkServerDeclaredExceptionClientInstruction;
use rpc::rpc::SinkServerDeclaredExceptionClientTestResult;
use rpc::rpc::SinkServerErrorClientInstruction;
use rpc::rpc::SinkServerErrorClientTestResult;
use rpc::rpc::SinkUndeclaredExceptionClientInstruction;
use rpc::rpc::SinkUndeclaredExceptionClientTestResult;
use rpc::rpc::StreamBasicClientInstruction;
use rpc::rpc::StreamBasicClientTestResult;
use rpc::rpc::StreamCancelClientInstruction;
use rpc::rpc::StreamCancelClientTestResult;
use rpc::rpc::StreamChunkTimeoutClientInstruction;
use rpc::rpc::StreamChunkTimeoutClientTestResult;
use rpc::rpc::StreamCreditTimeoutClientInstruction;
use rpc::rpc::StreamCreditTimeoutClientTestResult;
use rpc::rpc::StreamDeclaredExceptionClientInstruction;
use rpc::rpc::StreamDeclaredExceptionClientTestResult;
use rpc::rpc::StreamInitialDeclaredExceptionClientInstruction;
use rpc::rpc::StreamInitialDeclaredExceptionClientTestResult;
use rpc::rpc::StreamInitialResponseClientInstruction;
use rpc::rpc::StreamInitialResponseClientTestResult;
use rpc::rpc::StreamInitialTimeoutClientInstruction;
use rpc::rpc::StreamInitialTimeoutClientTestResult;
use rpc::rpc::StreamInitialUndeclaredExceptionClientInstruction;
use rpc::rpc::StreamInitialUndeclaredExceptionClientTestResult;
use rpc::rpc::StreamServerErrorClientInstruction;
use rpc::rpc::StreamServerErrorClientTestResult;
use rpc::rpc::StreamUndeclaredExceptionClientInstruction;
use rpc::rpc::StreamUndeclaredExceptionClientTestResult;
use rpc_clients::rpc::RPCConformanceServiceExt;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiMethodDeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiMethodUndeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiSinkDeclaredExceptionSinkError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiSinkUndeclaredExceptionSinkError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiStreamDeclaredExceptionStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiStreamUndeclaredExceptionStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::SinkChunkTimeoutSinkFinalError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::SinkDeclaredExceptionSinkError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::SinkUndeclaredExceptionSinkError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamChunkTimeoutStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamCreditTimeoutStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamDeclaredExceptionStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamInitialDeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamInitialTimeoutError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamInitialUndeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamUndeclaredExceptionStreamError;
use rpc_clients::rpc_setup::errors::r_p_c_conformance_setup_service::CreateRPCConformanceServiceClientError;
use rpc_nativeclients::rpc::RPCConformanceServiceExtNativeClient;
use rpc_nativeclients::rpc::build_RPCConformanceService_ext_client_with_binary_protocol;
use rpc_nativeclients::rpc::build_RPCConformanceService_ext_client_with_compact_protocol;
use rpc_nativeclients::rpc_setup::build_RPCConformanceSetupService_ext_client_with_binary_protocol;
use rpc_nativeclients::rpc_setup::build_RPCConformanceSetupService_ext_client_with_compact_protocol;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Parser, Debug)]
struct Arguments {
    #[clap(short, long, default_value = "7777")]
    port: u16,
    #[clap(long, default_value = "compact")]
    protocol: String,
    #[clap(long, default_value = "rocket")]
    transport: String,
    #[clap(long)]
    tls: bool,
    #[clap(long)]
    tls_cert: Option<String>,
    #[clap(long)]
    tls_key: Option<String>,
    #[clap(long)]
    tls_ca: Option<String>,
    /// nativeclient always verifies the checksum a response carries
    #[clap(long = "checksum")]
    _checksum: bool,
    #[clap(short, long, default_value = "info")]
    log: Vec<Directive>,
}

#[cli::main("rpc-client-native")]
async fn main(args: Arguments) -> Result<()> {
    init_logging(args.log.clone());
    let peer = Peer {
        port: args.port,
        protocol: match args.protocol.as_str() {
            "compact" => nativeclient::WireProtocol::Compact,
            "binary" => nativeclient::WireProtocol::Binary,
            other => anyhow::bail!("Unknown protocol {}", other),
        },
        transport: match args.transport.as_str() {
            "rocket" => nativeclient::TransportType::Rocket,
            "header" => nativeclient::TransportType::Header,
            "http2" => nativeclient::TransportType::Http,
            other => anyhow::bail!("Unknown transport {}", other),
        },
        tls: if args.tls {
            Some(Tls {
                cert_path: args
                    .tls_cert
                    .ok_or_else(|| anyhow::anyhow!("--tls needs --tls-cert"))?,
                key_path: args
                    .tls_key
                    .ok_or_else(|| anyhow::anyhow!("--tls needs --tls-key"))?,
                ca_path: args
                    .tls_ca
                    .ok_or_else(|| anyhow::anyhow!("--tls needs --tls-ca"))?,
            })
        } else {
            None
        },
    };
    let client = client_of(peer.protocol, peer.channel_builder()?)?;
    test(&*client, &peer).await?;

    Ok(())
}

struct Peer {
    port: u16,
    protocol: nativeclient::WireProtocol,
    transport: nativeclient::TransportType,
    tls: Option<Tls>,
}

struct Tls {
    cert_path: String,
    key_path: String,
    ca_path: String,
}

impl Peer {
    fn channel_builder(&self) -> Result<nativeclient::NativeChannelBuilder> {
        let address = SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), self.port);
        let builder = nativeclient::NativeChannelBuilder::from_sock_addr(address)?
            .with_secure(false)
            .with_transport_type(self.transport)
            .with_http_host("localhost")
            .with_http_url("/")
            .with_conn_timeout(30_000)
            .with_recv_timeout(30_000);
        Ok(match &self.tls {
            Some(tls) => builder
                .with_secure(true)
                .with_certificate_paths(nativeclient::CertificatePaths {
                    cert_path: tls.cert_path.clone(),
                    key_path: tls.key_path.clone(),
                    ca_path: tls.ca_path.clone(),
                })
                .with_enforce_peer_identity(false),
            None => builder,
        })
    }
}

fn client_of(
    protocol: nativeclient::WireProtocol,
    builder: nativeclient::NativeChannelBuilder,
) -> Result<RPCConformanceServiceExtNativeClient> {
    match protocol {
        nativeclient::WireProtocol::Binary => {
            build_RPCConformanceService_ext_client_with_binary_protocol(builder)
        }
        nativeclient::WireProtocol::Compact => {
            build_RPCConformanceService_ext_client_with_compact_protocol(builder)
        }
    }
}

fn init_logging(directives: Vec<Directive>) {
    let fmt = tracing_subscriber::fmt::Layer::default()
        .with_writer(std::io::stderr)
        .event_format(Glog::default().with_timer(tracing_glog::LocalTime::default()))
        .fmt_fields(GlogFields::default());

    let filter = directives
        .into_iter()
        .fold(EnvFilter::from_default_env(), |filter, directive| {
            filter.add_directive(directive)
        });

    let subscriber = Registry::default().with(filter).with(fmt);
    tracing::subscriber::set_global_default(subscriber).expect("to set global subscriber");
}

// --

use rpc::rpc::CallKind;
use rpc::rpc::ChecksumKind;
use rpc::rpc::ClientInstruction;
use rpc::rpc::ClientTestResult;
use rpc::rpc::ClosingFrameClientInstruction;
use rpc::rpc::ClosingFrameClientTestResult;
use rpc::rpc::InteractionConstructorClientInstruction;
use rpc::rpc::InteractionConstructorClientTestResult;
use rpc::rpc::InteractionFactoryFunctionClientInstruction;
use rpc::rpc::InteractionFactoryFunctionClientTestResult;
use rpc::rpc::InteractionPersistsStateClientInstruction;
use rpc::rpc::InteractionPersistsStateClientTestResult;
use rpc::rpc::InteractionTerminationClientInstruction;
use rpc::rpc::InteractionTerminationClientTestResult;
use rpc::rpc::RequestResponseAnyExceptionClientInstruction;
use rpc::rpc::RequestResponseAnyExceptionClientTestResult;
use rpc::rpc::RequestResponseBadContentLengthClientInstruction;
use rpc::rpc::RequestResponseBadContentLengthClientTestResult;
use rpc::rpc::RequestResponseBasicClientInstruction;
use rpc::rpc::RequestResponseBasicClientTestResult;
use rpc::rpc::RequestResponseConcurrentClientInstruction;
use rpc::rpc::RequestResponseConcurrentClientTestResult;
use rpc::rpc::RequestResponseDeclaredExceptionClientInstruction;
use rpc::rpc::RequestResponseDeclaredExceptionClientTestResult;
use rpc::rpc::RequestResponseMetadataClientInstruction;
use rpc::rpc::RequestResponseMetadataClientTestResult;
use rpc::rpc::RequestResponseNoArgVoidResponseClientTestResult;
use rpc::rpc::RequestResponseServerErrorClientInstruction;
use rpc::rpc::RequestResponseServerErrorClientTestResult;
use rpc::rpc::RequestResponseTimeoutClientInstruction;
use rpc::rpc::RequestResponseTimeoutClientTestResult;
use rpc::rpc::RequestResponseUndeclaredExceptionClientInstruction;
use rpc::rpc::RequestResponseUndeclaredExceptionClientTestResult;
use rpc_clients::rpc::BasicInteraction;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiBasicError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::BidiBasicStreamError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::RequestResponseBasicError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::RequestResponseDeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::RequestResponseTimeoutError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::RequestResponseUndeclaredExceptionError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::SinkBasicError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::SinkBasicSinkFinalError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamBasicError;
use rpc_clients::rpc::errors::r_p_c_conformance_service::StreamBasicStreamError;
use standard::StandardProtocol;
use type_rep::ProtocolUnion;

async fn test(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    peer: &Peer,
) -> Result<()> {
    use ClientInstruction::*;

    match &client.getTestCase().await?.clientInstruction {
        requestResponseBasic(i) => request_response_basic(client, i).await,
        requestResponseDeclaredException(i) => request_response_declared_exn(client, i).await,
        requestResponseUndeclaredException(i) => request_response_undeclared_exn(client, i).await,
        requestResponseNoArgVoidResponse(_) => request_response_no_arg_void_response(client).await,
        requestResponseTimeout(i) => request_response_timeout(client, i).await,
        requestResponseAnyException(i) => request_response_any_exception(client, i).await,
        requestResponseServerError(i) => request_response_server_error(client, peer, i).await,
        requestResponseMetadata(i) => request_response_metadata(client, peer, i).await,
        requestResponseConcurrent(i) => request_response_concurrent(client, peer, i).await,
        requestResponseBadContentLength(i) => request_response_bad_content_length(client, i).await,
        interactionConstructor(i) => interaction_constructor(client, i).await,
        interactionFactoryFunction(i) => interaction_factory_function(client, i).await,
        interactionPersistsState(i) => interaction_persists_state(client, i).await,
        interactionTermination(i) => interaction_termination(client, i).await,
        streamBasic(i) => stream_basic(client, i).await,
        streamChunkTimeout(i) => stream_chunk_timeout(client, i).await,
        streamInitialResponse(i) => stream_initial_response(client, i).await,
        streamCreditTimeout(i) => stream_credit_timeout(client, i).await,
        streamDeclaredException(i) => stream_declared_exception(client, i).await,
        streamUndeclaredException(i) => stream_undeclared_exception(client, i).await,
        streamInitialDeclaredException(i) => stream_initial_declared_exception(client, i).await,
        streamInitialUndeclaredException(i) => stream_initial_undeclared_exception(client, i).await,
        streamInitialTimeout(i) => stream_initial_timeout(client, i).await,
        streamCancel(i) => stream_cancel(client, i).await,
        streamServerError(i) => stream_server_error(client, i).await,
        sinkBasic(i) => sink_basic(client, i).await,
        sinkChunkTimeout(i) => sink_chunk_timeout(client, i).await,
        sinkAbort(i) => sink_abort(client, i).await,
        sinkServerError(i) => sink_server_error(client, i).await,
        sinkInitialResponse(i) => sink_initial_response(client, i).await,
        sinkInitialDeclaredException(i) => sink_initial_declared_exception(client, i).await,
        sinkDeclaredException(i) => sink_declared_exception(client, i).await,
        sinkServerDeclaredException(i) => sink_server_declared_exception(client, i).await,
        sinkUndeclaredException(i) => sink_undeclared_exception(client, i).await,
        bidiBasic(i) => bidi_basic(client, i).await,
        bidiInitialResponse(i) => bidi_initial_response(client, i).await,
        bidiStreamDeclaredException(i) => bidi_stream_declared_exception(client, i).await,
        bidiStreamUndeclaredException(i) => bidi_stream_undeclared_exception(client, i).await,
        bidiSinkDeclaredException(i) => bidi_sink_declared_exception(client, i).await,
        bidiSinkUndeclaredException(i) => bidi_sink_undeclared_exception(client, i).await,
        bidiMethodDeclaredException(i) => bidi_method_declared_exception(client, i).await,
        bidiMethodUndeclaredException(i) => bidi_method_undeclared_exception(client, i).await,
        bidiAbort(i) => bidi_abort(client, i).await,
        bidiServerError(i) => bidi_server_error(client, i).await,
        closingFrame(i) => closing_frame(peer, i).await,
        UnknownField(i) => Err(anyhow!(format!("not supported: {:?}", i))),
    }
}

// ---

fn timed_out(error: &anyhow::Error) -> bool {
    matches!(
        error.thrift_error(),
        Some(nativeclient::ThriftError::Transport(
            nativeclient::TransportError::Timeout(_)
                | nativeclient::TransportError::StreamTimeout { .. }
        ))
    )
}

/// mirrors `TTransportException::UNKNOWN`
const UNKNOWN: i32 = 0;
/// mirrors `TTransportException::NOT_OPEN`
const NOT_OPEN: i32 = 1;

fn transport_exception_type(error: &anyhow::Error) -> Option<i32> {
    match error.thrift_error()? {
        nativeclient::ThriftError::Transport(nativeclient::TransportError::Http2(
            nativeclient::Http2Error::Reset { .. },
        )) => Some(UNKNOWN),
        nativeclient::ThriftError::Transport(
            nativeclient::TransportError::Closed(_) | nativeclient::TransportError::Http2(_),
        ) => Some(NOT_OPEN),
        _ => None,
    }
}

async fn create_interaction(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    initial_sum: &Option<i32>,
) -> Result<Arc<dyn BasicInteraction>> {
    Ok(match initial_sum {
        Some(value) => client.basicInteractionFactoryFunction(*value).await?,
        None => client.createBasicInteraction()? as Arc<_>,
    })
}

async fn drop_unread<F: Future + Unpin>(mut call: F, after_ms: i64) {
    let _ = (&mut call).now_or_never();
    tokio::time::sleep(Duration::from_millis(after_ms as u64)).await;
}

fn checksum_of(kind: Option<ChecksumKind>) -> nativeclient::Checksum {
    match kind {
        Some(ChecksumKind::CRC32) => nativeclient::Checksum::Crc32,
        Some(ChecksumKind::XXH3_64) => nativeclient::Checksum::Xxh3_64,
        _ => nativeclient::Checksum::None,
    }
}

// ---

async fn request_response_basic(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseBasicClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_checksum(checksum_of(instr.checksum));
    let test_result =
        ClientTestResult::requestResponseBasic(RequestResponseBasicClientTestResult {
            response: client
                .requestResponseBasic_with_rpc_opts(&instr.request, opts)
                .await?,
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn request_response_declared_exn(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseDeclaredExceptionClientInstruction,
) -> Result<()> {
    match &client
        .requestResponseDeclaredException(&instr.request)
        .await
    {
        Err(RequestResponseDeclaredExceptionError::e(exn)) => {
            let test_result = ClientTestResult::requestResponseDeclaredException(
                RequestResponseDeclaredExceptionClientTestResult {
                    userException: Some(Box::new(exn.clone())),
                    ..Default::default()
                },
            );
            client.sendTestResult(&test_result).await?;
            Ok(())
        }
        r => Err(anyhow!(
            "request_response_declared_exception: unexpected server response:  {:?}",
            r
        )),
    }
}

async fn request_response_undeclared_exn(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseUndeclaredExceptionClientInstruction,
) -> Result<()> {
    match &client
        .requestResponseUndeclaredException(&instr.request)
        .await
    {
        Err(RequestResponseUndeclaredExceptionError::ApplicationException(exn)) => {
            let test_result = ClientTestResult::requestResponseUndeclaredException(
                RequestResponseUndeclaredExceptionClientTestResult {
                    exceptionMessage: exn.message.clone(),
                    ..Default::default()
                },
            );
            client.sendTestResult(&test_result).await?;
            Ok(())
        }
        r => Err(anyhow!(
            "request_response_undeclared_exception: unexpected server response: {:?}",
            r
        )),
    }
}

/// The exception arrives inside a Thrift `Any`. The call fails with its message
/// as it would for any undeclared exception, and the `Any` itself is read off
/// the response observation and decoded with the protocol it names.
async fn request_response_any_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseAnyExceptionClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    let response = opts.observe_response();
    let exception_message = match client
        .requestResponseUndeclaredException_with_rpc_opts(&instr.request, opts)
        .await
    {
        Err(RequestResponseUndeclaredExceptionError::ApplicationException(exn)) => exn.message,
        r => {
            return Err(anyhow!(
                "request_response_any_exception: unexpected server response: {:?}",
                r
            ));
        }
    };
    let observation = response
        .observation()
        .ok_or_else(|| anyhow!("request_response_any_exception: no reply was observed"))?;
    let any = observation
        .any_exception()
        .ok_or_else(|| anyhow!("request_response_any_exception: the reply carried no Any"))?;
    let data = &any.any().data;
    let user_exception = match &any.any().protocol {
        ProtocolUnion::standard(StandardProtocol::Compact) => {
            fbthrift::compact_protocol::deserialize(data)?
        }
        ProtocolUnion::standard(StandardProtocol::Binary) => {
            fbthrift::binary_protocol::deserialize(data)?
        }
        other => {
            return Err(anyhow!(
                "request_response_any_exception: the Any names protocol {:?}",
                other
            ));
        }
    };
    let test_result = ClientTestResult::requestResponseAnyException(
        RequestResponseAnyExceptionClientTestResult {
            exceptionMessage: exception_message,
            typeUri: any.type_uri().unwrap_or_default().to_owned(),
            userException: Some(Box::new(user_exception)),
            ..Default::default()
        },
    );
    client.sendTestResult(&test_result).await?;
    Ok(())
}

/// The server rejects the call itself, with one of the internal server errors,
/// and the call fails with the `ApplicationException` its error code maps to.
async fn request_response_server_error(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    peer: &Peer,
    instr: &RequestResponseServerErrorClientInstruction,
) -> Result<()> {
    let exn = if instr.unknownMethod {
        unknown_method(peer).await?
    } else {
        match client.requestResponseBasic(&instr.request).await {
            Err(RequestResponseBasicError::ApplicationException(exn)) => exn,
            r => {
                return Err(anyhow!(
                    "request_response_server_error: unexpected server response: {:?}",
                    r
                ));
            }
        }
    };
    let test_result =
        ClientTestResult::requestResponseServerError(RequestResponseServerErrorClientTestResult {
            exceptionType: exn.type_ as i32,
            exceptionMessage: exn.message,
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn unknown_method(peer: &Peer) -> Result<fbthrift::ApplicationException> {
    let builder = peer.channel_builder()?;
    let setup = match peer.protocol {
        nativeclient::WireProtocol::Binary => {
            build_RPCConformanceSetupService_ext_client_with_binary_protocol(builder)?
        }
        nativeclient::WireProtocol::Compact => {
            build_RPCConformanceSetupService_ext_client_with_compact_protocol(builder)?
        }
    };
    match setup.createRPCConformanceServiceClient("", "").await {
        Err(CreateRPCConformanceServiceClientError::ApplicationException(exn)) => Ok(exn),
        r => Err(anyhow!(
            "unknown_method: unexpected server response: {:?}",
            r
        )),
    }
}

async fn request_response_metadata(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    peer: &Peer,
    instr: &RequestResponseMetadataClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    for (key, value) in &instr.writeHeaders {
        opts.set_write_header(key, value);
    }
    if let Some(timeout) = instr.timeoutMs {
        opts.set_timeout_ms(timeout.try_into()?);
    }
    if let Some(timeout) = instr.queueTimeoutMs {
        opts.set_queue_timeout_ms(timeout.try_into()?);
    }
    opts.set_client_only_timeouts(instr.clientOnlyTimeouts);
    if let Some(priority) = instr.priority {
        opts.set_priority(match priority {
            0 => nativeclient::Priority::HighImportant,
            1 => nativeclient::Priority::High,
            2 => nativeclient::Priority::Important,
            3 => nativeclient::Priority::Normal,
            4 => nativeclient::Priority::BestEffort,
            p => anyhow::bail!("Unknown priority {}", p),
        });
    }
    let mut builder = peer.channel_builder()?;
    for header in &instr.persistentHeaders {
        builder = builder.with_persistent_header(header);
    }
    let response = client_of(peer.protocol, builder)?
        .requestResponseBasic_with_rpc_opts(&instr.request, opts)
        .await?;
    let test_result =
        ClientTestResult::requestResponseMetadata(RequestResponseMetadataClientTestResult {
            response,
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn request_response_concurrent(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    peer: &Peer,
    instr: &RequestResponseConcurrentClientInstruction,
) -> Result<()> {
    let one_connection = client_of(
        peer.protocol,
        peer.channel_builder()?.with_channel_pool(false),
    )?;
    let responses = futures::future::try_join_all(
        instr
            .requests
            .iter()
            .map(|request| one_connection.requestResponseBasic(request)),
    )
    .await?;
    let test_result =
        ClientTestResult::requestResponseConcurrent(RequestResponseConcurrentClientTestResult {
            responses,
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn request_response_no_arg_void_response(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
) -> Result<()> {
    let () = client.requestResponseNoArgVoidResponse().await?;
    let test_result = ClientTestResult::requestResponseNoArgVoidResponse(
        RequestResponseNoArgVoidResponseClientTestResult {
            ..Default::default()
        },
    );
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn request_response_timeout(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseTimeoutClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_timeout_ms(instr.timeoutMs as u32);
    let result = client
        .requestResponseTimeout_with_rpc_opts(&instr.request, opts)
        .await;

    match &result {
        Err(RequestResponseTimeoutError::ThriftError(exn)) => {
            tracing::info!("thrift error: {:?}", exn);
            let test_result =
                ClientTestResult::requestResponseTimeout(RequestResponseTimeoutClientTestResult {
                    timeoutException: timed_out(exn),
                    ..Default::default()
                });
            client.sendTestResult(&test_result).await?;
            Ok(())
        }
        r => Err(anyhow!(
            "request_response_timeout: unexpected server response {:?}",
            r
        )),
    }
}

async fn request_response_bad_content_length(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &RequestResponseBadContentLengthClientInstruction,
) -> Result<()> {
    let transport_exception_type = match client.requestResponseBasic(&instr.request).await {
        Err(RequestResponseBasicError::ThriftError(exn)) => {
            tracing::info!("thrift error: {:?}", exn);
            transport_exception_type(&exn)
        }
        _ => None,
    };
    let test_result = ClientTestResult::requestResponseBadContentLength(
        RequestResponseBadContentLengthClientTestResult {
            transportExceptionType: transport_exception_type,
            ..Default::default()
        },
    );
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn interaction_constructor(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    _instr: &InteractionConstructorClientInstruction,
) -> Result<()> {
    let i = client.createBasicInteraction()?;
    let () = i.init().await?;
    let test_result =
        ClientTestResult::interactionConstructor(InteractionConstructorClientTestResult {
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn interaction_factory_function(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &InteractionFactoryFunctionClientInstruction,
) -> Result<()> {
    let _i = client
        .basicInteractionFactoryFunction(instr.initialSum)
        .await?;
    let test_result =
        ClientTestResult::interactionFactoryFunction(InteractionFactoryFunctionClientTestResult {
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn interaction_persists_state(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &InteractionPersistsStateClientInstruction,
) -> Result<()> {
    let i = create_interaction(client, &instr.initialSum).await?;
    let mut test_result: InteractionPersistsStateClientTestResult = Default::default();
    for value in &instr.valuesToAdd {
        test_result.responses.push(i.add(*value).await?);
    }
    client
        .sendTestResult(&ClientTestResult::interactionPersistsState(test_result))
        .await?;
    Ok(())
}

async fn interaction_termination(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &InteractionTerminationClientInstruction,
) -> Result<()> {
    let i = create_interaction(client, &instr.initialSum).await?;
    let () = i.init().await?;
    drop(i);
    let test_result =
        ClientTestResult::interactionTermination(InteractionTerminationClientTestResult {
            ..Default::default()
        });
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn stream_basic(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamBasicClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    match &instr.memoryBufferSize {
        Some(size) => {
            opts.set_memory_buffer_size(
                size.targetBytes.try_into()?,
                size.initialChunks,
                size.maxChunks,
            );
        }
        None => {
            opts.set_chunk_buffer_size(instr.bufferSize as i32);
        }
    }
    opts.set_checksum(checksum_of(instr.checksum));

    let mut stream = client
        .streamBasic_with_rpc_opts(&instr.request, opts)
        .await?;
    let mut test_result = StreamBasicClientTestResult {
        streamPayloads: vec![],
        ..Default::default()
    };
    while let Some(response) = stream.next().await {
        test_result.streamPayloads.push(response?);
    }
    client
        .sendTestResult(&ClientTestResult::streamBasic(test_result))
        .await?;
    Ok(())
}

async fn stream_server_error(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamServerErrorClientInstruction,
) -> Result<()> {
    let exn = match client.streamBasic(&instr.request).await {
        Err(StreamBasicError::ApplicationException(exn)) => Some(exn),
        Err(other) => {
            return Err(anyhow!(
                "stream_server_error: unexpected error: {:?}",
                other
            ));
        }
        Ok(mut stream) => loop {
            match stream.next().await {
                None => break None,
                Some(Ok(_)) => {}
                Some(Err(StreamBasicStreamError::ApplicationException(exn))) => break Some(exn),
                Some(Err(other)) => {
                    return Err(anyhow!(
                        "stream_server_error: unexpected stream error: {:?}",
                        other
                    ));
                }
            }
        },
    };
    let test_result = ClientTestResult::streamServerError(exn.map_or_else(
        StreamServerErrorClientTestResult::default,
        |exn| StreamServerErrorClientTestResult {
            exceptionType: exn.type_ as i32,
            exceptionMessage: exn.message,
            ..Default::default()
        },
    ));
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn stream_cancel(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamCancelClientInstruction,
) -> Result<()> {
    let call = client.streamBasic(&instr.request);
    let mut test_result = StreamCancelClientTestResult::default();
    match (instr.giveUpAfterMs, instr.dropUnreadAfterMs) {
        (Some(give_up_after_ms), _) => {
            drop(tokio::time::timeout(Duration::from_millis(give_up_after_ms as u64), call).await)
        }
        (None, Some(drop_unread_after_ms)) => drop_unread(call, drop_unread_after_ms).await,
        (None, None) => {
            let mut stream = call.await?;
            while (test_result.streamPayloads.len() as i64) < instr.consumeCount {
                match stream.next().await {
                    Some(response) => test_result.streamPayloads.push(response?),
                    None => break,
                }
            }
            drop(stream);
        }
    }
    client
        .sendTestResult(&ClientTestResult::streamCancel(test_result))
        .await?;
    Ok(())
}

async fn stream_chunk_timeout(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamChunkTimeoutClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_chunk_timeout_ms(instr.chunkTimeoutMs as u32);

    let mut stream = client
        .streamChunkTimeout_with_rpc_opts(&instr.request, opts)
        .await?;
    let mut test_result = StreamChunkTimeoutClientTestResult {
        chunkTimeoutException: false,
        streamPayloads: vec![],
        ..Default::default()
    };

    while let Some(response) = stream.next().await {
        match response {
            Ok(r) => {
                test_result.streamPayloads.push(r);
            }
            Err(StreamChunkTimeoutStreamError::ThriftError(exn)) => {
                test_result.chunkTimeoutException |= timed_out(&exn);
            }
            Err(other) => {
                return Err(anyhow!(
                    "stream_chunk_timeout: unexpected stream error: {:?}",
                    other
                ));
            }
        }
    }

    client
        .sendTestResult(&ClientTestResult::streamChunkTimeout(test_result))
        .await?;
    Ok(())
}

async fn stream_initial_response(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamInitialResponseClientInstruction,
) -> Result<()> {
    let (initial_response, mut stream) = client.streamInitialResponse(&instr.request).await?;
    let mut test_result = StreamInitialResponseClientTestResult {
        initialResponse: initial_response,
        streamPayloads: vec![],
        ..Default::default()
    };

    while let Some(response) = stream.next().await {
        test_result.streamPayloads.push(response?);
    }

    client
        .sendTestResult(&ClientTestResult::streamInitialResponse(test_result))
        .await?;

    Ok(())
}

async fn stream_credit_timeout(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamCreditTimeoutClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_chunk_buffer_size(0);
    let mut stream = client
        .streamCreditTimeout_with_rpc_opts(&instr.request, opts)
        .await?;
    let first = stream.next().await;
    tracing::info!("stream_credit_timeout: first item {:?}", first);
    let mut credit_timeout = false;
    if let Some(Ok(_)) = first {
        // Sleep longer than the stream expiration time so that the server
        // will run out of credit and throw a credit timeout exception
        tokio::time::sleep(Duration::from_millis(instr.creditTimeoutMs as u64)).await;
        let second = stream.next().await;
        tracing::info!("stream_credit_timeout: second item {:?}", second);
        credit_timeout = matches!(
            second,
            Some(Err(StreamCreditTimeoutStreamError::ApplicationException(aexn)))
                if aexn.type_ == fbthrift::ApplicationExceptionErrorCode::Timeout
        );
    }
    let test_result = StreamCreditTimeoutClientTestResult {
        creditTimeoutException: credit_timeout,
        ..Default::default()
    };

    client
        .sendTestResult(&ClientTestResult::streamCreditTimeout(test_result))
        .await?;

    Ok(())
}

async fn stream_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamDeclaredExceptionClientInstruction,
) -> Result<()> {
    let mut stream = client.streamDeclaredException(&instr.request).await?;
    let mut test_result = StreamDeclaredExceptionClientTestResult {
        userException: None,
        ..Default::default()
    };
    while let Some(result) = stream.next().await {
        if let Err(StreamDeclaredExceptionStreamError::e(user_error)) = result {
            test_result.userException = Some(Box::new(user_error));
        };
    }
    client
        .sendTestResult(&ClientTestResult::streamDeclaredException(test_result))
        .await?;
    Ok(())
}

async fn stream_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let mut stream = client.streamUndeclaredException(&instr.request).await?;
    let mut test_result = StreamUndeclaredExceptionClientTestResult {
        exceptionMessage: "".to_string(),
        ..Default::default()
    };
    while let Some(result) = stream.next().await {
        match result {
            Err(StreamUndeclaredExceptionStreamError::ApplicationException(aexn)) => {
                test_result.exceptionMessage = aexn.message;
            }
            Err(other) => tracing::info!("stream_undeclared_exception: untyped error {:?}", other),
            Ok(_) => {}
        }
    }
    client
        .sendTestResult(&ClientTestResult::streamUndeclaredException(test_result))
        .await?;
    Ok(())
}

async fn stream_initial_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamInitialDeclaredExceptionClientInstruction,
) -> Result<()> {
    let result = client.streamInitialDeclaredException(&instr.request).await;
    let mut test_result = StreamInitialDeclaredExceptionClientTestResult {
        userException: None,
        ..Default::default()
    };
    if let Err(StreamInitialDeclaredExceptionError::e(user_error)) = result {
        test_result.userException = Some(Box::new(user_error));
    };
    client
        .sendTestResult(&ClientTestResult::streamInitialDeclaredException(
            test_result,
        ))
        .await?;
    Ok(())
}

async fn stream_initial_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamInitialUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let result = client
        .streamInitialUndeclaredException(&instr.request)
        .await;
    let mut test_result = StreamInitialUndeclaredExceptionClientTestResult {
        exceptionMessage: "".to_string(),
        ..Default::default()
    };
    match result {
        Err(StreamInitialUndeclaredExceptionError::ApplicationException(aexn)) => {
            test_result.exceptionMessage = aexn.message;
        }
        Err(other) => tracing::info!(
            "stream_initial_undeclared_exception: untyped error {:?}",
            other
        ),
        Ok(_) => {}
    }
    client
        .sendTestResult(&ClientTestResult::streamInitialUndeclaredException(
            test_result,
        ))
        .await?;
    Ok(())
}

async fn stream_initial_timeout(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &StreamInitialTimeoutClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_timeout_ms(instr.timeoutMs as u32);
    let result = client
        .streamInitialTimeout_with_rpc_opts(&instr.request, opts)
        .await;
    let mut test_result = StreamInitialTimeoutClientTestResult {
        timeoutException: false,
        ..Default::default()
    };
    match &result {
        Err(StreamInitialTimeoutError::ThriftError(exn)) => {
            test_result.timeoutException = timed_out(exn);
        }
        Err(other) => tracing::info!("stream_initial_timeout: other error {:?}", other),
        Ok(_) => {}
    }
    client
        .sendTestResult(&ClientTestResult::streamInitialTimeout(test_result))
        .await?;
    Ok(())
}

async fn sink_basic(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkBasicClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_checksum(checksum_of(instr.checksum));
    let sink_result = client.sinkBasic_with_rpc_opts(&instr.request, opts).await?;
    let final_response = (sink_result.sink)(
        futures::stream::iter(instr.sinkPayloads.clone())
            .map(Ok)
            .boxed(),
    )
    .await
    .map_err(|err| anyhow!("sink_basic: sink failed: {:?}", err))?;

    let test_result = SinkBasicClientTestResult {
        finalResponse: final_response,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkBasic(test_result))
        .await?;

    Ok(())
}

async fn sink_abort(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkAbortClientInstruction,
) -> Result<()> {
    let call = client.sinkBasic(&instr.request);
    match (instr.giveUpAfterMs, instr.dropUnreadAfterMs) {
        (Some(give_up_after_ms), _) => {
            drop(tokio::time::timeout(Duration::from_millis(give_up_after_ms as u64), call).await)
        }
        (None, Some(drop_unread_after_ms)) => drop_unread(call, drop_unread_after_ms).await,
        (None, None) => drop(call.await?),
    }
    client
        .sendTestResult(&ClientTestResult::sinkAbort(
            SinkAbortClientTestResult::default(),
        ))
        .await?;
    Ok(())
}

async fn sink_server_error(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkServerErrorClientInstruction,
) -> Result<()> {
    let exn = match client.sinkBasic(&instr.request).await {
        Err(SinkBasicError::ApplicationException(exn)) => Some(exn),
        Err(other) => {
            return Err(anyhow!("sink_server_error: unexpected error: {:?}", other));
        }
        Ok(sink_result) => match (sink_result.sink)(futures::stream::empty().boxed()).await {
            Ok(_) => None,
            Err(SinkBasicSinkFinalError::ApplicationException(exn)) => Some(exn),
            Err(other) => {
                return Err(anyhow!(
                    "sink_server_error: unexpected final error: {:?}",
                    other
                ));
            }
        },
    };
    let test_result = ClientTestResult::sinkServerError(exn.map_or_else(
        SinkServerErrorClientTestResult::default,
        |exn| SinkServerErrorClientTestResult {
            exceptionType: exn.type_ as i32,
            exceptionMessage: exn.message,
            ..Default::default()
        },
    ));
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn sink_chunk_timeout(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkChunkTimeoutClientInstruction,
) -> Result<()> {
    let sink_result = client.sinkChunkTimeout(&instr.request).await?;
    let pause = futures::stream::once(tokio::time::sleep(Duration::from_millis(
        instr.chunkTimeoutMs.try_into()?,
    )))
    .filter_map(|()| async { None });
    let final_response = (sink_result.sink)(
        futures::stream::iter(instr.sinkPayloads.clone())
            .map(Ok)
            .chain(pause)
            .boxed(),
    )
    .await;

    let chunk_timeout = match final_response {
        Ok(_) => false,
        Err(SinkChunkTimeoutSinkFinalError::ApplicationException(aexn))
            if aexn.type_ == fbthrift::ApplicationExceptionErrorCode::Timeout =>
        {
            true
        }
        _ => false,
    };

    let test_result = SinkChunkTimeoutClientTestResult {
        chunkTimeoutException: chunk_timeout,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkChunkTimeout(test_result))
        .await?;

    Ok(())
}

async fn sink_initial_response(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkInitialResponseClientInstruction,
) -> Result<()> {
    let sink_result = client.sinkInitialResponse(&instr.request).await?;
    let final_response = (sink_result.sink)(
        futures::stream::iter(instr.sinkPayloads.clone())
            .map(Ok)
            .boxed(),
    )
    .await?;

    let test_result = SinkInitialResponseClientTestResult {
        finalResponse: final_response,
        initialResponse: sink_result.initial_response,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkInitialResponse(test_result))
        .await?;

    Ok(())
}

async fn sink_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkDeclaredExceptionClientInstruction,
) -> Result<()> {
    let sink_result = client.sinkDeclaredException(&instr.request).await?;
    let final_response = if let Some(ue) = &instr.userException {
        (sink_result.sink)(
            futures::stream::iter([ue.clone()])
                .map(|ex| Err(SinkDeclaredExceptionSinkError::e(ex)))
                .boxed(),
        )
        .await
    } else {
        (sink_result.sink)(futures::stream::empty().boxed()).await
    };

    if instr.userException.is_some() {
        assert!(final_response.is_err());
    } else {
        assert!(final_response.is_ok());
    }

    let test_result = SinkDeclaredExceptionClientTestResult {
        sinkThrew: instr.userException.is_some(),
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkDeclaredException(test_result))
        .await?;

    Ok(())
}

async fn sink_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let sink_result = client.sinkUndeclaredException(&instr.request).await?;
    let final_response = if let Some(ue) = &instr.exceptionMessage {
        (sink_result.sink)(
            futures::stream::iter([ue.clone()])
                .map(|ex| {
                    Err(SinkUndeclaredExceptionSinkError::ApplicationException(
                        fbthrift::ApplicationException::new(
                            fbthrift::ApplicationExceptionErrorCode::Unknown,
                            ex,
                        ),
                    ))
                })
                .boxed(),
        )
        .await
    } else {
        (sink_result.sink)(futures::stream::empty().boxed()).await
    };

    if instr.exceptionMessage.is_some() {
        assert!(final_response.is_err());
    } else {
        assert!(final_response.is_ok());
    }

    let test_result = SinkUndeclaredExceptionClientTestResult {
        sinkThrew: instr.exceptionMessage.is_some(),
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkUndeclaredException(test_result))
        .await?;

    Ok(())
}
async fn sink_initial_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkInitialDeclaredExceptionClientInstruction,
) -> Result<()> {
    let result = client.sinkInitialDeclaredException(&instr.request).await;

    let sink_threw = match result {
        Err(_) => true,
        Ok(_) => {
            tracing::info!("sink_initial_declared_exception: the call opened a sink");
            false
        }
    };

    let test_result = SinkInitialDeclaredExceptionClientTestResult {
        sinkThrew: sink_threw,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkInitialDeclaredException(test_result))
        .await?;
    Ok(())
}

async fn sink_server_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &SinkServerDeclaredExceptionClientInstruction,
) -> Result<()> {
    let sink_result = client.sinkServerDeclaredException(&instr.request).await?;
    let final_response = (sink_result.sink)(futures::stream::empty().boxed()).await;
    let sink_threw = final_response.is_err();

    let test_result = SinkServerDeclaredExceptionClientTestResult {
        sinkThrew: sink_threw,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::sinkServerDeclaredException(test_result))
        .await?;
    Ok(())
}

// =================== Bidi ===================

/// The bidi transport prefixes the exception type to the message, and the
/// expected result recorded in the test suite carries only the message. C++
/// trims it the same way and with the same crudeness, first `": "` wins, so a
/// message that contains one of its own would be trimmed too
/// (`RPCClient.cpp:517-522`).
fn without_type_prefix(message: &str) -> String {
    match message.split_once(": ") {
        Some((_, rest)) => rest.to_string(),
        None => message.to_string(),
    }
}

async fn bidi_basic(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiBasicClientInstruction,
) -> Result<()> {
    let mut opts = nativeclient::RpcOptions::default();
    opts.set_checksum(checksum_of(instr.checksum));
    let (stream, sink) = client.bidiBasic_with_rpc_opts(&instr.request, opts).await?;
    let payloads = instr.sinkPayloads.clone();
    let (sink_result, stream_payloads) = futures::future::join(
        (sink)(futures::stream::iter(payloads).map(Ok).boxed()),
        stream.collect::<Vec<_>>(),
    )
    .await;
    sink_result.map_err(|err| anyhow!("bidi_basic: sink failed: {:?}", err))?;

    let test_result = BidiBasicClientTestResult {
        streamPayloads: stream_payloads
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| anyhow!("bidi_basic: stream failed: {:?}", err))?,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::bidiBasic(test_result))
        .await?;
    Ok(())
}

async fn bidi_initial_response(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiInitialResponseClientInstruction,
) -> Result<()> {
    let (initial_response, stream, sink) = client.bidiInitialResponse(&instr.request).await?;
    let payloads = instr.sinkPayloads.clone();
    let (sink_result, stream_payloads) = futures::future::join(
        (sink)(futures::stream::iter(payloads).map(Ok).boxed()),
        stream.collect::<Vec<_>>(),
    )
    .await;
    sink_result.map_err(|err| anyhow!("bidi_initial_response: sink failed: {:?}", err))?;

    let test_result = BidiInitialResponseClientTestResult {
        initialResponse: initial_response,
        streamPayloads: stream_payloads
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| anyhow!("bidi_initial_response: stream failed: {:?}", err))?,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::bidiInitialResponse(test_result))
        .await?;
    Ok(())
}

async fn bidi_stream_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiStreamDeclaredExceptionClientInstruction,
) -> Result<()> {
    let (stream, sink) = client.bidiStreamDeclaredException(&instr.request).await?;
    let payloads = instr.sinkPayloads.clone();
    let mut test_result = BidiStreamDeclaredExceptionClientTestResult {
        ..Default::default()
    };
    let collect = async {
        let mut stream = stream;
        let mut payloads = vec![];
        let mut thrown = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(response) => payloads.push(response),
                Err(BidiStreamDeclaredExceptionStreamError::e(user_error)) => {
                    thrown = Some(Box::new(user_error));
                }
                Err(_) => {}
            }
        }
        (payloads, thrown)
    };
    let (_sink_result, (stream_payloads, thrown)) = futures::future::join(
        (sink)(futures::stream::iter(payloads).map(Ok).boxed()),
        collect,
    )
    .await;

    test_result.streamPayloads = stream_payloads;
    test_result.userException = thrown;
    client
        .sendTestResult(&ClientTestResult::bidiStreamDeclaredException(test_result))
        .await?;
    Ok(())
}

async fn bidi_stream_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiStreamUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let (stream, sink) = client.bidiStreamUndeclaredException(&instr.request).await?;
    let payloads = instr.sinkPayloads.clone();
    let collect = async {
        let mut stream = stream;
        let mut payloads = vec![];
        let mut message = String::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(response) => payloads.push(response),
                Err(BidiStreamUndeclaredExceptionStreamError::ApplicationException(aexn)) => {
                    message = without_type_prefix(&aexn.message)
                }
                Err(other) => tracing::info!(
                    "bidi_stream_undeclared_exception: untyped error {:?}",
                    other
                ),
            }
        }
        (payloads, message)
    };
    let (_sink_result, (stream_payloads, exception_message)) = futures::future::join(
        (sink)(futures::stream::iter(payloads).map(Ok).boxed()),
        collect,
    )
    .await;

    let test_result = BidiStreamUndeclaredExceptionClientTestResult {
        streamPayloads: stream_payloads,
        exceptionMessage: exception_message,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::bidiStreamUndeclaredException(
            test_result,
        ))
        .await?;
    Ok(())
}

async fn bidi_sink_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiSinkDeclaredExceptionClientInstruction,
) -> Result<()> {
    let (stream, sink) = client.bidiSinkDeclaredException(&instr.request).await?;
    let payloads = instr.sinkPayloads.clone();
    let thrown = instr.userException.clone();
    let items = futures::stream::iter(payloads)
        .map(Ok)
        .chain(futures::stream::iter(
            thrown
                .into_iter()
                .map(|exn| Err(BidiSinkDeclaredExceptionSinkError::e(*exn))),
        ))
        .boxed();
    let (sink_result, stream_payloads) =
        futures::future::join((sink)(items), stream.collect::<Vec<_>>()).await;

    let test_result = BidiSinkDeclaredExceptionClientTestResult {
        sinkThrew: sink_result.is_err(),
        streamPayloads: stream_payloads
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| anyhow!("bidi_sink_declared_exception: stream failed: {:?}", err))?,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::bidiSinkDeclaredException(test_result))
        .await?;
    Ok(())
}

async fn bidi_sink_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiSinkUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let (stream, sink) = client.bidiSinkUndeclaredException(&instr.request).await?;
    let payloads = instr.sinkPayloads.clone();
    let thrown = instr.exceptionMessage.clone();
    let items = futures::stream::iter(payloads)
        .map(Ok)
        .chain(futures::stream::iter(thrown.into_iter().map(|message| {
            Err(BidiSinkUndeclaredExceptionSinkError::ApplicationException(
                fbthrift::ApplicationException::new(
                    fbthrift::ApplicationExceptionErrorCode::Unknown,
                    message,
                ),
            ))
        })))
        .boxed();
    let (sink_result, stream_payloads) =
        futures::future::join((sink)(items), stream.collect::<Vec<_>>()).await;

    let test_result = BidiSinkUndeclaredExceptionClientTestResult {
        sinkThrew: sink_result.is_err(),
        streamPayloads: stream_payloads
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| anyhow!("bidi_sink_undeclared_exception: stream failed: {:?}", err))?,
        ..Default::default()
    };
    client
        .sendTestResult(&ClientTestResult::bidiSinkUndeclaredException(test_result))
        .await?;
    Ok(())
}

async fn bidi_method_declared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiMethodDeclaredExceptionClientInstruction,
) -> Result<()> {
    let mut test_result = BidiMethodDeclaredExceptionClientTestResult {
        ..Default::default()
    };
    if let Err(BidiMethodDeclaredExceptionError::e(user_error)) =
        client.bidiMethodDeclaredException(&instr.request).await
    {
        test_result.userException = Some(Box::new(user_error));
    }
    client
        .sendTestResult(&ClientTestResult::bidiMethodDeclaredException(test_result))
        .await?;
    Ok(())
}

async fn bidi_method_undeclared_exception(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiMethodUndeclaredExceptionClientInstruction,
) -> Result<()> {
    let mut test_result = BidiMethodUndeclaredExceptionClientTestResult {
        ..Default::default()
    };
    match client.bidiMethodUndeclaredException(&instr.request).await {
        Err(BidiMethodUndeclaredExceptionError::ApplicationException(aexn)) => {
            test_result.exceptionMessage = without_type_prefix(&aexn.message);
        }
        Err(other) => tracing::info!(
            "bidi_method_undeclared_exception: untyped error {:?}",
            other
        ),
        Ok(_) => {}
    }
    client
        .sendTestResult(&ClientTestResult::bidiMethodUndeclaredException(
            test_result,
        ))
        .await?;
    Ok(())
}

async fn bidi_abort(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiAbortClientInstruction,
) -> Result<()> {
    let call = client.bidiBasic(&instr.request);
    match (instr.giveUpAfterMs, instr.dropUnreadAfterMs) {
        (Some(give_up_after_ms), _) => {
            drop(tokio::time::timeout(Duration::from_millis(give_up_after_ms as u64), call).await)
        }
        (None, Some(drop_unread_after_ms)) => drop_unread(call, drop_unread_after_ms).await,
        (None, None) => drop(call.await?),
    }
    client
        .sendTestResult(&ClientTestResult::bidiAbort(
            BidiAbortClientTestResult::default(),
        ))
        .await?;
    Ok(())
}

async fn bidi_server_error(
    client: &dyn RPCConformanceServiceExt<nativeclient::NativeChannel>,
    instr: &BidiServerErrorClientInstruction,
) -> Result<()> {
    let exn = match client.bidiBasic(&instr.request).await {
        Err(BidiBasicError::ApplicationException(exn)) => Some(exn),
        Err(other) => {
            return Err(anyhow!("bidi_server_error: unexpected error: {:?}", other));
        }
        Ok((mut stream, _sink)) => loop {
            match stream.next().await {
                None => break None,
                Some(Ok(_)) => {}
                Some(Err(BidiBasicStreamError::ApplicationException(exn))) => break Some(exn),
                Some(Err(other)) => {
                    return Err(anyhow!(
                        "bidi_server_error: unexpected stream error: {:?}",
                        other
                    ));
                }
            }
        },
    };
    let test_result = ClientTestResult::bidiServerError(exn.map_or_else(
        BidiServerErrorClientTestResult::default,
        |exn| BidiServerErrorClientTestResult {
            exceptionType: exn.type_ as i32,
            exceptionMessage: exn.message,
            ..Default::default()
        },
    ));
    client.sendTestResult(&test_result).await?;
    Ok(())
}

async fn closing_frame(peer: &Peer, instr: &ClosingFrameClientInstruction) -> Result<()> {
    let one_connection = client_of(
        peer.protocol,
        peer.channel_builder()?.with_channel_pool(false),
    )?;
    let failed = match instr.call {
        CallKind::REQUEST_RESPONSE => one_connection
            .requestResponseBasic(&instr.request)
            .await
            .is_err(),
        CallKind::STREAM => one_connection.streamBasic(&instr.request).await.is_err(),
        CallKind::SINK => one_connection.sinkBasic(&instr.request).await.is_err(),
        CallKind::BIDI => one_connection.bidiBasic(&instr.request).await.is_err(),
        other => return Err(anyhow!("closing_frame: unknown call {:?}", other)),
    };
    if !failed {
        return Err(anyhow!("closing_frame: the call succeeded"));
    }
    one_connection
        .sendTestResult(&ClientTestResult::closingFrame(
            ClosingFrameClientTestResult::default(),
        ))
        .await?;
    Ok(())
}
