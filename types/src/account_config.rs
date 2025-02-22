// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unit_arg)]

use crate::{
    access_path::{AccessPath, Accesses},
    account_address::AccountAddress,
    account_state_blob::AccountStateBlob,
    byte_array::ByteArray,
    event::EventHandle,
    language_storage::StructTag,
};
use canonical_serialization::{
    CanonicalDeserialize, CanonicalDeserializer, CanonicalSerialize, CanonicalSerializer,
    SimpleDeserializer,
};
use failure::prelude::*;
use lazy_static::lazy_static;
#[cfg(any(test, feature = "testing"))]
use proptest_derive::Arbitrary;
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
};

/// An account object. This is the top-level entry in global storage. We'll never need to create an
/// `Account` struct, but if we did, it would look something like
/// pub struct Account {
/// // Address holding this account
/// address: Address,
/// // Struct types defined by this account
/// code: HashMap<Name, Code>,
/// // Resurces owned by this account
/// resoruces: HashMap<ObjectName, StructInstance>,
/// }

// LibraCoin
pub const COIN_MODULE_NAME: &str = "LibraCoin";
pub const COIN_STRUCT_NAME: &str = "T";

// Account
pub const ACCOUNT_MODULE_NAME: &str = "LibraAccount";
pub const ACCOUNT_STRUCT_NAME: &str = "T";

// Hash
pub const HASH_MODULE_NAME: &str = "Hash";

pub fn core_code_address() -> AccountAddress {
    AccountAddress::default()
}

pub fn association_address() -> AccountAddress {
    AccountAddress::from_hex_literal("0xA550C18")
        .expect("Parsing valid hex literal should always succeed")
}

pub fn coin_struct_tag() -> StructTag {
    StructTag {
        module: COIN_MODULE_NAME.to_string(),
        name: COIN_STRUCT_NAME.to_string(),
        address: core_code_address(),
        type_params: vec![],
    }
}

pub fn account_struct_tag() -> StructTag {
    StructTag {
        address: core_code_address(),
        module: ACCOUNT_MODULE_NAME.to_string(),
        name: ACCOUNT_STRUCT_NAME.to_string(),
        type_params: vec![],
    }
}

/// A Rust representation of an Account resource.
/// This is not how the Account is represented in the VM but it's a convenient representation.
#[derive(Debug, Default)]
#[cfg_attr(any(test, feature = "testing"), derive(Arbitrary))]
pub struct AccountResource {
    balance: u64,
    sequence_number: u64,
    authentication_key: ByteArray,
    delegated_withdrawal_capability: bool,
    sent_events: EventHandle,
    received_events: EventHandle,
}

impl AccountResource {
    /// Constructs an Account resource.
    pub fn new(
        balance: u64,
        sequence_number: u64,
        authentication_key: ByteArray,
        delegated_withdrawal_capability: bool,
        sent_events: EventHandle,
        received_events: EventHandle,
    ) -> Self {
        AccountResource {
            balance,
            sequence_number,
            authentication_key,
            delegated_withdrawal_capability,
            sent_events,
            received_events,
        }
    }

    /// Given an account map (typically from storage) retrieves the Account resource associated.
    pub fn make_from(account_map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Self> {
        let ap = account_resource_path();
        match account_map.get(&ap) {
            Some(bytes) => SimpleDeserializer::deserialize(bytes),
            None => bail!("No data for {:?}", ap),
        }
    }

    /// Return the sequence_number field for the given AccountResource
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Return the balance field for the given AccountResource
    pub fn balance(&self) -> u64 {
        self.balance
    }

    /// Return the authentication_key field for the given AccountResource
    pub fn authentication_key(&self) -> &ByteArray {
        &self.authentication_key
    }

    /// Return the sent_events handle for the given AccountResource
    pub fn sent_events(&self) -> &EventHandle {
        &self.sent_events
    }

    /// Return the received_events handle for the given AccountResource
    pub fn received_events(&self) -> &EventHandle {
        &self.received_events
    }

    /// Return the delegated_withdrawal_capability field for the given AccountResource
    pub fn delegated_withdrawal_capability(&self) -> bool {
        self.delegated_withdrawal_capability
    }

    pub fn get_event_handle_by_query_path(&self, query_path: &[u8]) -> Result<&EventHandle> {
        if *ACCOUNT_RECEIVED_EVENT_PATH == query_path {
            Ok(&self.received_events)
        } else if *ACCOUNT_SENT_EVENT_PATH == query_path {
            Ok(&self.sent_events)
        } else {
            bail!("Unrecognized query path: {:?}", query_path);
        }
    }
}

impl CanonicalSerialize for AccountResource {
    fn serialize(&self, serializer: &mut impl CanonicalSerializer) -> Result<()> {
        // TODO(drussi): the order in which these fields are serialized depends on some
        // implementation details in the VM.
        serializer
            .encode_struct(&self.authentication_key)?
            .encode_u64(self.balance)?
            .encode_bool(self.delegated_withdrawal_capability)?
            .encode_struct(&self.received_events)?
            .encode_struct(&self.sent_events)?
            .encode_u64(self.sequence_number)?;
        Ok(())
    }
}

impl CanonicalDeserialize for AccountResource {
    fn deserialize(deserializer: &mut impl CanonicalDeserializer) -> Result<Self> {
        let authentication_key = deserializer.decode_struct()?;
        let balance = deserializer.decode_u64()?;
        let delegated_withdrawal_capability = deserializer.decode_bool()?;
        let received_events = deserializer.decode_struct()?;
        let sent_events = deserializer.decode_struct()?;
        let sequence_number = deserializer.decode_u64()?;

        Ok(AccountResource {
            balance,
            sequence_number,
            authentication_key,
            delegated_withdrawal_capability,
            sent_events,
            received_events,
        })
    }
}

pub fn get_account_resource_or_default(
    account_state: &Option<AccountStateBlob>,
) -> Result<AccountResource> {
    match account_state {
        Some(blob) => {
            let account_btree = blob.try_into()?;
            AccountResource::make_from(&account_btree)
        }
        None => Ok(AccountResource::default()),
    }
}

/// Return the path to the Account resource. It can be used to create an AccessPath for an
/// Account resource.
pub fn account_resource_path() -> Vec<u8> {
    AccessPath::resource_access_vec(&account_struct_tag(), &Accesses::empty())
}

lazy_static! {
    /// The path to the sent event counter for an Account resource.
    /// It can be used to query the event DB for the given event.
    pub static ref ACCOUNT_SENT_EVENT_PATH: Vec<u8> = {
        let mut path = account_resource_path();
        path.extend_from_slice(b"/sent_events_count/");
        path
    };

    /// Returns the path to the received event counter for an Account resource.
    /// It can be used to query the event DB for the given event.
    pub static ref ACCOUNT_RECEIVED_EVENT_PATH: Vec<u8> = {
        let mut path = account_resource_path();
        path.extend_from_slice(b"/received_events_count/");
        path
    };
}

/// Generic struct that represents an Account event.
/// Both SentPaymentEvent and ReceivedPaymentEvent are representable with this struct.
/// They have an AccountAddress for the sender or receiver and the amount transferred.
#[derive(Debug, Default)]
pub struct AccountEvent {
    account: AccountAddress,
    amount: u64,
}

impl AccountEvent {
    pub fn try_from(bytes: &[u8]) -> Result<AccountEvent> {
        let mut deserializer = SimpleDeserializer::new(bytes);
        let amount = deserializer.decode_u64()?;
        let offset = deserializer.position() as usize;
        Ok(Self {
            account: AccountAddress::try_from(&bytes[offset..])?,
            amount,
        })
    }

    /// Get the account related to the event
    pub fn account(&self) -> AccountAddress {
        self.account
    }

    /// Get the amount sent or received
    pub fn amount(&self) -> u64 {
        self.amount
    }
}
