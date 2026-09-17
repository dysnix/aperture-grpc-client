# aperture-grpc-client

Rust client for Aperture's lightweight decoded transaction gRPC stream.

It wraps the generated [`aperture-grpc-proto`](https://github.com/dysnix/aperture-grpc-proto)
bindings with tuned HTTP/2 defaults, keepalives, byte-safe filters, and an
automatic reconnecting transaction stream. It supports both single-transaction
and batched txstream RPCs.

## Transaction versions

Version 0.5 adds v1 transactions and `TransactionConfig`. V1 is delivered
by default on both RPCs with the existing subscription request: no opt-in or
request-schema change is needed. Upgrade generated clients to understand the
new enum value and config; older protobuf decoders ignore the additional field.

`TransactionVersion` values remain `Legacy = 0`, `V0 = 1`; `V1 = 2` is new.
Full v1 responses carry `transaction_config`, even if all its fields are absent.
Priority fee is **total lamports**. Absent CU and loaded account data limits
mean zero; absent heap size means 32 KiB. Legacy/v0 have no config. In v1 all
accounts are static; no ALT addresses are loaded. Signatures-only responses
retain the version but omit config along with the message payload.

## Install

```toml
[dependencies]
aperture-grpc-client = "0.6.1"
```

For unreleased development builds:

```toml
[dependencies]
aperture-grpc-client = { git = "https://github.com/dysnix/aperture-grpc-client" }
```

## Example

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default().vote(VoteFilter::NonVoteOnly);
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let tx = next?;
        println!(
            "slot={} index={} sigs={} alt_resolution={:?}",
            tx.slot,
            tx.index,
            tx.signatures.len(),
            tx.alt_resolution
        );
    }

    Ok(())
}
```

`DecodedTransaction.alt_resolution` is `Some("FULL")` when the emitted account
list is complete, `Some("PARTIAL")` when one or more ALT entries could not be
resolved, and `None` when the server does not provide resolution status. The
field is additive on the wire, so older protobuf clients continue to decode
responses and ignore it.

For lower per-message overhead, subscribe to batches:

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default()
        .vote(VoteFilter::NonVoteOnly)
        .signatures_only();
    let mut stream = Box::pin(client.subscribe_batches_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let batch = next?;
        println!("transactions={}", batch.transactions.len());
    }

    Ok(())
}
```

Request real-time transaction simulation when you need a predicted execution
result alongside each deshredded transaction:

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SimulationStatus, SubscribeFilters,
    VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default()
        .vote(VoteFilter::NonVoteOnly)
        .include_simulation();
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let tx = next?;
        if let Some(simulation) = tx.simulation {
            let status = SimulationStatus::try_from(simulation.status)
                .unwrap_or(SimulationStatus::Unspecified);
            println!(
                "slot={} status={status:?} bank_slot={:?} error={:?}",
                tx.slot, simulation.bank_slot, simulation.error
            );
        }
    }

    Ok(())
}
```

Run the included example:

```bash
APERTURE_GRPC_ENDPOINT=https://aperture-txstream.rpcfast.com:443 APERTURE_X_TOKEN=rpcfast-token cargo run --example subscribe
```

## Defaults

- endpoint: `https://aperture-txstream.rpcfast.com:443`
- TCP connect timeout: 3s
- HTTP/2 keepalive interval: 10s
- HTTP/2 keepalive timeout: 3s
- keepalive while idle: enabled
- TCP_NODELAY: enabled
- HTTP/2 adaptive window: enabled
- initial stream and connection windows: 16 MiB
- protobuf encode/decode limits: 16 MiB
- HTTPS endpoints use native TLS trust roots with the `ring` crypto provider by default
- authentication metadata: optional `X-Token` header only
- reconnect: enabled forever, exponential backoff from 100ms to 5s

## Filters

Filters use raw Solana bytes:

- `signature`: optional 64-byte primary signature filter.
- `account_include`: 32-byte pubkeys, match any static or loaded account.
- `account_exclude`: 32-byte pubkeys, reject if any static or loaded account matches.
- `account_required`: 32-byte pubkeys, require all listed accounts.
- `vote`: all, vote-only, or non-vote-only.
- `signatures_only`: omit account/instruction payloads and keep only
  slot/index/vote/timestamp/version/signatures.
- `include_simulation`: wait for transaction simulation and append status,
  error, simulation slot, and timing to each transaction.
  It can be combined with `signatures_only` for a compact
  signature-and-result stream.

Instruction indexes are resolved by concatenating:

```text
static_account_keys + loaded_writable_addresses + loaded_readonly_addresses
```

The stream is pre-execution by default and does not include confirmed execution
metadata such as balances, rewards, or inner instructions. An
`include_simulation` subscription adds predicted simulation status, error,
simulation slot, and timing; it is not confirmation or finality.

## Simulation details

`include_simulation()` returns status, error, compute units, bank slot and timing.
Logs and other details are disabled by default.

Use `simulation_config()` to choose additional fields:

```rust,no_run
use aperture_grpc_client::{SimulationConfig, SimulationInclude, SubscribeFilters};

let filters = SubscribeFilters::default().simulation_config(SimulationConfig {
    include: Some(SimulationInclude {
        compute_units: true,
        token_balance_deltas: true,
        logs: true,
        ..Default::default()
    }),
    ..Default::default()
});
```

An explicit config enables simulation on either RPC and takes precedence over
`include_simulation`. All `SimulationInclude` flags default to false:
`compute_units`, `account_deltas`, `token_balance_deltas`, `inner_instructions`,
`logs` and `return_data`.

Deltas contain only changed writable accounts. Account states contain metadata,
without raw data; token amounts are raw integers without decimals or UI amounts.
CPI instructions carry resolved program/account keys and stack height.
Optional `SimulationConfig.account_include` and `owner_include` filters select
deltas; empty lists allow all accounts. These options also work with
`signatures_only`.

See the [proto documentation](https://docs.rs/aperture-grpc-proto/0.6.1)
for response fields and filter semantics.
