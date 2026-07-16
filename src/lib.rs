#![doc = include_str!("../README.md")]

#[cfg(feature = "tls-native-roots")]
use tonic::transport::ClientTlsConfig;
use {
    aperture_grpc_proto::aperture_client::ApertureClient,
    async_stream::stream,
    futures_core::Stream,
    std::{sync::Arc, time::Duration},
    tonic::{
        Request, Status,
        codec::Streaming,
        metadata::{Ascii, MetadataValue},
        transport::{Channel, Endpoint},
    },
};

pub use aperture_grpc_proto::{
    CompiledInstruction, DecodedTransaction, DecodedTransactionBatch, MessageHeader,
    SimulationStatus, SubscribeTransactionsRequest, TransactionReturnData, TransactionSimulation,
    TransactionVersion, VoteFilter, aperture_client, aperture_server,
};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const X_TOKEN_METADATA_KEY: &str = "x-token";

/// Client configuration for Aperture's lightweight decoded transaction stream.
#[derive(Debug, Clone)]
pub struct ApertureClientConfig {
    /// Endpoint URI, for example `https://aperture-grpc.rpcfast.com:443`.
    pub endpoint: String,
    /// TCP/connect timeout.
    pub connect_timeout: Duration,
    /// Optional per-request timeout. Keep this unset for long-lived streams.
    pub request_timeout: Option<Duration>,
    /// Enable TCP_NODELAY.
    pub tcp_nodelay: bool,
    /// HTTP/2 keepalive PING interval.
    pub http2_keep_alive_interval: Option<Duration>,
    /// HTTP/2 keepalive PING ack timeout.
    pub http2_keep_alive_timeout: Option<Duration>,
    /// Send HTTP/2 keepalives even when the connection is idle.
    pub keep_alive_while_idle: bool,
    /// Enable HTTP/2 adaptive flow-control window.
    pub http2_adaptive_window: bool,
    /// Initial per-stream HTTP/2 flow-control window.
    pub initial_stream_window_size: Option<u32>,
    /// Initial connection-level HTTP/2 flow-control window.
    pub initial_connection_window_size: Option<u32>,
    /// Optional max received HTTP/2 header list size.
    pub http2_max_header_list_size: Option<u32>,
    /// Max decoded protobuf message size.
    pub max_decoding_message_size: usize,
    /// Max encoded protobuf message size.
    pub max_encoding_message_size: usize,
    /// Optional user-agent.
    pub user_agent: Option<String>,
    /// Optional `X-Token` authentication header sent as gRPC metadata `x-token`.
    pub x_token: Option<String>,
    /// Automatic reconnection policy for [`ApertureGrpcClient::subscribe_with_reconnect`].
    pub reconnect: ReconnectConfig,
}

impl ApertureClientConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    pub fn with_x_token(mut self, token: impl Into<String>) -> Self {
        self.x_token = Some(token.into());
        self
    }
}

impl Default for ApertureClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://aperture-grpc.rpcfast.com:443".to_string(),
            connect_timeout: Duration::from_secs(3),
            request_timeout: None,
            tcp_nodelay: true,
            http2_keep_alive_interval: Some(Duration::from_secs(10)),
            http2_keep_alive_timeout: Some(Duration::from_secs(3)),
            keep_alive_while_idle: true,
            http2_adaptive_window: true,
            initial_stream_window_size: Some(DEFAULT_MAX_MESSAGE_SIZE as u32),
            initial_connection_window_size: Some(DEFAULT_MAX_MESSAGE_SIZE as u32),
            http2_max_header_list_size: None,
            max_decoding_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_encoding_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            user_agent: Some(format!(
                "aperture-grpc-client/{}",
                env!("CARGO_PKG_VERSION")
            )),
            x_token: None,
            reconnect: ReconnectConfig::default(),
        }
    }
}

/// Exponential reconnect backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectConfig {
    pub enabled: bool,
    pub min_delay: Duration,
    pub max_delay: Duration,
    /// Maximum reconnect attempts after a disconnect or failed connection.
    /// `None` retries forever.
    pub max_attempts: Option<u32>,
}

impl ReconnectConfig {
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(10);
        let multiplier = 1u32 << shift;
        self.min_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            max_attempts: None,
        }
    }
}

/// SubscribeTransactions filters using raw Solana bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscribeFilters {
    pub vote: VoteFilter,
    pub signature: Option<[u8; 64]>,
    pub account_include: Vec<[u8; 32]>,
    pub account_exclude: Vec<[u8; 32]>,
    pub account_required: Vec<[u8; 32]>,
    pub signatures_only: bool,
    /// Wait for transaction simulation and append the result to each transaction.
    pub include_simulation: bool,
}

impl SubscribeFilters {
    pub fn vote(mut self, vote: VoteFilter) -> Self {
        self.vote = vote;
        self
    }

    pub fn signature(mut self, signature: [u8; 64]) -> Self {
        self.signature = Some(signature);
        self
    }

    pub fn include_account(mut self, account: [u8; 32]) -> Self {
        self.account_include.push(account);
        self
    }

    pub fn exclude_account(mut self, account: [u8; 32]) -> Self {
        self.account_exclude.push(account);
        self
    }

    pub fn require_account(mut self, account: [u8; 32]) -> Self {
        self.account_required.push(account);
        self
    }

    pub fn signatures_only(mut self) -> Self {
        self.signatures_only = true;
        self
    }

    pub fn with_signatures_only(mut self, signatures_only: bool) -> Self {
        self.signatures_only = signatures_only;
        self
    }

    pub fn include_simulation(mut self) -> Self {
        self.include_simulation = true;
        self
    }

    pub fn with_include_simulation(mut self, include_simulation: bool) -> Self {
        self.include_simulation = include_simulation;
        self
    }
}

impl From<SubscribeFilters> for SubscribeTransactionsRequest {
    fn from(filters: SubscribeFilters) -> Self {
        Self {
            vote: filters.vote as i32,
            signature: filters.signature.map(Vec::from).unwrap_or_default(),
            account_include: filters.account_include.into_iter().map(Vec::from).collect(),
            account_exclude: filters.account_exclude.into_iter().map(Vec::from).collect(),
            account_required: filters
                .account_required
                .into_iter()
                .map(Vec::from)
                .collect(),
            signatures_only: filters.signatures_only,
            include_simulation: filters.include_simulation,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApertureGrpcClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status: {0}")]
    Status(#[from] Status),
    #[error("invalid X-Token metadata value: {0}")]
    InvalidXTokenMetadata(String),
    #[error("reconnect attempts exhausted after {attempts} attempts")]
    ReconnectAttemptsExhausted { attempts: u32 },
}

/// High-level Aperture gRPC client with tuned HTTP/2 defaults and reconnects.
#[derive(Debug, Clone)]
pub struct ApertureGrpcClient {
    config: Arc<ApertureClientConfig>,
}

impl ApertureGrpcClient {
    pub fn new(config: ApertureClientConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    pub fn config(&self) -> &ApertureClientConfig {
        &self.config
    }

    pub async fn subscribe_once(
        &self,
        filters: SubscribeFilters,
    ) -> Result<Streaming<DecodedTransaction>, ApertureGrpcClientError> {
        let mut client = self.connect_once().await?;
        let mut request = Request::new(filters.into());
        apply_x_token(self.config.x_token.as_deref(), &mut request)?;
        Ok(client.subscribe_transactions(request).await?.into_inner())
    }

    pub async fn subscribe_batches_once(
        &self,
        filters: SubscribeFilters,
    ) -> Result<Streaming<DecodedTransactionBatch>, ApertureGrpcClientError> {
        let mut client = self.connect_once().await?;
        let mut request = Request::new(filters.into());
        apply_x_token(self.config.x_token.as_deref(), &mut request)?;
        Ok(client
            .subscribe_transaction_batches(request)
            .await?
            .into_inner())
    }

    pub fn subscribe_with_reconnect(
        &self,
        filters: SubscribeFilters,
    ) -> impl Stream<Item = Result<DecodedTransaction, ApertureGrpcClientError>> + Send + 'static
    {
        let client = self.clone();
        stream! {
            let mut attempt = 0u32;
            loop {
                match client.subscribe_once(filters.clone()).await {
                    Ok(mut grpc_stream) => {
                        attempt = 0;
                        loop {
                            match grpc_stream.message().await {
                                Ok(Some(transaction)) => {
                                    attempt = 0;
                                    yield Ok(transaction);
                                }
                                Ok(None) => break,
                                Err(status) => {
                                    if !client.config.reconnect.enabled {
                                        yield Err(ApertureGrpcClientError::Status(status));
                                        return;
                                    }
                                    tracing::warn!(%status, "Aperture gRPC stream failed; reconnecting");
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        if !client.config.reconnect.enabled {
                            yield Err(error);
                            return;
                        }
                        tracing::warn!(%error, "Aperture gRPC subscribe failed; reconnecting");
                    }
                }

                if !client.config.reconnect.enabled {
                    return;
                }

                attempt = attempt.saturating_add(1);
                if let Some(max_attempts) = client.config.reconnect.max_attempts
                    && attempt > max_attempts
                {
                    yield Err(ApertureGrpcClientError::ReconnectAttemptsExhausted {
                        attempts: max_attempts,
                    });
                    return;
                }

                let delay = client.config.reconnect.delay_for_attempt(attempt);
                tracing::info!(attempt, ?delay, "reconnecting Aperture gRPC stream");
                tokio::time::sleep(delay).await;
            }
        }
    }

    pub fn subscribe_batches_with_reconnect(
        &self,
        filters: SubscribeFilters,
    ) -> impl Stream<Item = Result<DecodedTransactionBatch, ApertureGrpcClientError>> + Send + 'static
    {
        let client = self.clone();
        stream! {
            let mut attempt = 0u32;
            loop {
                match client.subscribe_batches_once(filters.clone()).await {
                    Ok(mut grpc_stream) => {
                        attempt = 0;
                        loop {
                            match grpc_stream.message().await {
                                Ok(Some(batch)) => {
                                    attempt = 0;
                                    yield Ok(batch);
                                }
                                Ok(None) => break,
                                Err(status) => {
                                    if !client.config.reconnect.enabled {
                                        yield Err(ApertureGrpcClientError::Status(status));
                                        return;
                                    }
                                    tracing::warn!(%status, "Aperture gRPC batch stream failed; reconnecting");
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        if !client.config.reconnect.enabled {
                            yield Err(error);
                            return;
                        }
                        tracing::warn!(%error, "Aperture gRPC batch subscribe failed; reconnecting");
                    }
                }

                if !client.config.reconnect.enabled {
                    return;
                }

                attempt = attempt.saturating_add(1);
                if let Some(max_attempts) = client.config.reconnect.max_attempts
                    && attempt > max_attempts
                {
                    yield Err(ApertureGrpcClientError::ReconnectAttemptsExhausted {
                        attempts: max_attempts,
                    });
                    return;
                }

                let delay = client.config.reconnect.delay_for_attempt(attempt);
                tracing::info!(attempt, ?delay, "reconnecting Aperture gRPC batch stream");
                tokio::time::sleep(delay).await;
            }
        }
    }

    async fn connect_once(&self) -> Result<ApertureClient<Channel>, ApertureGrpcClientError> {
        let channel = build_endpoint(&self.config)?.connect().await?;
        Ok(ApertureClient::new(channel)
            .max_decoding_message_size(self.config.max_decoding_message_size)
            .max_encoding_message_size(self.config.max_encoding_message_size))
    }
}

fn build_endpoint(config: &ApertureClientConfig) -> Result<Endpoint, ApertureGrpcClientError> {
    let mut endpoint = Endpoint::from_shared(config.endpoint.clone())?
        .connect_timeout(config.connect_timeout)
        .tcp_nodelay(config.tcp_nodelay)
        .http2_adaptive_window(config.http2_adaptive_window)
        .keep_alive_while_idle(config.keep_alive_while_idle);

    if let Some(timeout) = config.request_timeout {
        endpoint = endpoint.timeout(timeout);
    }
    if let Some(interval) = config.http2_keep_alive_interval {
        endpoint = endpoint.http2_keep_alive_interval(interval);
    }
    if let Some(timeout) = config.http2_keep_alive_timeout {
        endpoint = endpoint.keep_alive_timeout(timeout);
    }
    if let Some(size) = config.initial_stream_window_size {
        endpoint = endpoint.initial_stream_window_size(size);
    }
    if let Some(size) = config.initial_connection_window_size {
        endpoint = endpoint.initial_connection_window_size(size);
    }
    if let Some(size) = config.http2_max_header_list_size {
        endpoint = endpoint.http2_max_header_list_size(size);
    }
    if let Some(user_agent) = &config.user_agent {
        endpoint = endpoint.user_agent(user_agent.clone())?;
    }
    #[cfg(feature = "tls-native-roots")]
    if endpoint_uses_https(&config.endpoint) {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }

    Ok(endpoint)
}

#[cfg(any(feature = "tls-native-roots", test))]
fn endpoint_uses_https(endpoint: &str) -> bool {
    endpoint
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

fn apply_x_token(
    x_token: Option<&str>,
    request: &mut Request<SubscribeTransactionsRequest>,
) -> Result<(), ApertureGrpcClientError> {
    if let Some(token) = x_token {
        let value = token
            .parse::<MetadataValue<Ascii>>()
            .map_err(|err| ApertureGrpcClientError::InvalidXTokenMetadata(err.to_string()))?;
        request.metadata_mut().insert(X_TOKEN_METADATA_KEY, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_convert_to_proto_request() {
        let request = SubscribeTransactionsRequest::from(
            SubscribeFilters::default()
                .vote(VoteFilter::NonVoteOnly)
                .signature([1; 64])
                .include_account([2; 32])
                .exclude_account([3; 32])
                .require_account([4; 32])
                .signatures_only()
                .include_simulation(),
        );

        assert_eq!(request.vote, VoteFilter::NonVoteOnly as i32);
        assert_eq!(request.signature, vec![1; 64]);
        assert_eq!(request.account_include, vec![vec![2; 32]]);
        assert_eq!(request.account_exclude, vec![vec![3; 32]]);
        assert_eq!(request.account_required, vec![vec![4; 32]]);
        assert!(request.signatures_only);
        assert!(request.include_simulation);
    }

    #[test]
    fn reconnect_backoff_caps_at_max_delay() {
        let reconnect = ReconnectConfig {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(350),
            ..ReconnectConfig::default()
        };

        assert_eq!(reconnect.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(reconnect.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(reconnect.delay_for_attempt(3), Duration::from_millis(350));
        assert_eq!(reconnect.delay_for_attempt(20), Duration::from_millis(350));
    }

    #[test]
    fn endpoint_scheme_detection_is_https_only() {
        assert!(endpoint_uses_https("https://aperture-grpc.rpcfast.com:443"));
        assert!(endpoint_uses_https("HTTPS://aperture-grpc.rpcfast.com:443"));
        assert!(!endpoint_uses_https("http://127.0.0.1:10000"));
        assert!(!endpoint_uses_https("grpc://aperture-grpc.rpcfast.com:443"));
    }

    #[test]
    fn x_token_is_the_only_configured_metadata() {
        let config = ApertureClientConfig::default().with_x_token("secret-token");
        let mut request = Request::new(SubscribeTransactionsRequest::default());

        apply_x_token(config.x_token.as_deref(), &mut request).expect("valid token");

        assert_eq!(
            request
                .metadata()
                .get(X_TOKEN_METADATA_KEY)
                .expect("x-token set")
                .to_str()
                .expect("ascii token"),
            "secret-token"
        );
        assert_eq!(request.metadata().len(), 1);
    }
}
