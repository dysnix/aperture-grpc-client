use {
    aperture_grpc_client::{
        ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
    },
    futures_util::StreamExt,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "aperture_grpc_client=info,subscribe=info".to_string()),
        )
        .init();

    let endpoint = std::env::var("APERTURE_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "https://aperture-txstream.rpcfast.com:443".to_string());
    let mut config = ApertureClientConfig::new(endpoint);
    if let Ok(token) = std::env::var("APERTURE_X_TOKEN") {
        config = config.with_x_token(token);
    }
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default().vote(VoteFilter::NonVoteOnly);
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let transaction = next?;
        let primary_signature = transaction
            .signatures
            .first()
            .map(|signature| bs58::encode(signature).into_string())
            .unwrap_or_else(|| "<missing>".to_string());
        println!(
            "slot={} index={} sig={} static_keys={} loaded_writable={} loaded_readonly={}",
            transaction.slot,
            transaction.index,
            primary_signature,
            transaction.static_account_keys.len(),
            transaction.loaded_writable_addresses.len(),
            transaction.loaded_readonly_addresses.len()
        );
    }

    Ok(())
}
