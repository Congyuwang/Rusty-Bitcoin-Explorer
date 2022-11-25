use bitcoin::blockdata::opcodes::{all, All};
use bitcoin::blockdata::script::Instruction;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::util::address::Payload;
use bitcoin::{Address, Network, PubkeyHash, PublicKey, Script};
use serde::{Deserialize, Serialize};
use std::fmt;
use Instruction::{Op, PushBytes};

///
/// Different types of bitcoin Scripts.
///
/// See [An Analysis of Non-standard Transactions](https://doi.org/10.3389/fbloc.2019.00007)
/// for a more detailed explanation.
///
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ScriptType {
    OpReturn,
    Pay2MultiSig,
    Pay2PublicKey,
    Pay2PublicKeyHash,
    Pay2ScriptHash,
    Pay2WitnessPublicKeyHash,
    Pay2WitnessScriptHash,
    WitnessProgram,
    Unspendable,
    NotRecognised,
}

///
/// `ScriptInfo` stores a list of addresses extracted from ScriptPubKey.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptInfo {
    pub addresses: Vec<Address>,
    pub pattern: ScriptType,
}

///
/// This function extract addresses and script type from Script.
///
pub fn evaluate_script(script: &Script, net: Network) -> ScriptInfo {
    let address = Address::from_script(script, net);
    if script.is_p2pk() {
        ScriptInfo::new(p2pk_to_address(script), ScriptType::Pay2PublicKey)
    } else if script.is_p2pkh() {
        ScriptInfo::new(address, ScriptType::Pay2PublicKeyHash)
    } else if script.is_p2sh() {
        ScriptInfo::new(address, ScriptType::Pay2ScriptHash)
    } else if script.is_v0_p2wpkh() {
        ScriptInfo::new(address, ScriptType::Pay2WitnessPublicKeyHash)
    } else if script.is_v0_p2wsh() {
        ScriptInfo::new(address, ScriptType::Pay2WitnessScriptHash)
    } else if script.is_witness_program() {
        ScriptInfo::new(address, ScriptType::WitnessProgram)
    } else if script.is_op_return() {
        ScriptInfo::new(address, ScriptType::OpReturn)
    } else if script.is_provably_unspendable() {
        ScriptInfo::new(address, ScriptType::Unspendable)
    } else if is_multisig(script) {
        ScriptInfo::from_vec(multisig_addresses(script), ScriptType::Pay2MultiSig)
    } else {
        ScriptInfo::new(address, ScriptType::NotRecognised)
    }
}

impl ScriptInfo {
    pub(crate) fn new(address: Option<Address>, pattern: ScriptType) -> Self {
        if let Some(address) = address {
            Self::from_vec(vec![address], pattern)
        } else {
            Self::from_vec(Vec::new(), pattern)
        }
    }

    pub(crate) fn from_vec(addresses: Vec<Address>, pattern: ScriptType) -> Self {
        Self { addresses, pattern }
    }
}

///
/// translated from Bitcoinj:
/// [isSentToMultisig()](https://github.com/bitcoinj/bitcoinj/blob/d3d5edbcbdb91b25de4df3b6ed6740d7e2329efc/core/src/main/java/org/bitcoinj/script/ScriptPattern.java#L225:L246)
fn is_multisig(script: &Script) -> bool {
    // Read OpCodes
    let mut chunks: Vec<Instruction> = Vec::new();
    for i in script.instructions() {
        if let Ok(i) = i {
            chunks.push(i);
        } else {
            return false;
        }
    }

    // At least four chunks
    if chunks.len() < 4 {
        return false;
    }

    // Must end in OP_CHECKMULTISIG[VERIFY].
    match chunks.last().unwrap() {
        PushBytes(_) => {
            return false;
        }
        Op(op) => {
            if !(op.eq(&all::OP_CHECKMULTISIG) || op.eq(&all::OP_CHECKMULTISIGVERIFY)) {
                return false;
            }
        }
    }

    // Second to last chunk must be an OP_N opcode and there should be that many data chunks (keys).
    let second_last_chunk = chunks.get(chunks.len() - 2).unwrap();
    if let Some(num_keys) = get_num_keys(second_last_chunk) {
        // check number of chunks
        if num_keys < 1 || (num_keys + 3) as usize != chunks.len() {
            return false;
        }
    } else {
        return false;
    }

    // the rest must be data (except the first and the last 2)
    for chunk in chunks.iter().skip(1).take(chunks.len() - 3) {
        if let Op(_) = chunk {
            return false;
        }
    }

    // First chunk must be an OP_N opcode too.
    if let Some(num_keys) = get_num_keys(chunks.first().unwrap()) {
        // check number of chunks
        if num_keys < 1 {
            return false;
        }
    } else {
        return false;
    }
    true
}

///
/// Obtain addresses for multisig transactions.
///
fn multisig_addresses(script: &Script) -> Vec<Address> {
    assert!(is_multisig(script));
    let ops: Vec<Instruction> = script
        .instructions()
        .filter_map(|o| o.ok())
        .collect();

    // obtain number of keys
    let num_keys = {
        if let Some(Op(op)) = ops.get(ops.len() - 2) {
            decode_from_op_n(op)
        } else {
            unreachable!()
        }
    };
    // read public keys
    let mut public_keys = Vec::with_capacity(num_keys as usize);
    for op in ops.iter().skip(1).take(num_keys as usize) {
        if let PushBytes(data) = op {
            match PublicKey::from_slice(data) {
                Ok(pk) => public_keys.push(Address {
                    payload: Payload::PubkeyHash(pk.pubkey_hash()),
                    network: Network::Bitcoin,
                }),
                Err(_) => return Vec::new(),
            }
        } else {
            unreachable!()
        }
    }
    public_keys
}

///
/// Decode OP_N
///
/// translated from BitcoinJ:
/// [decodeFromOpN()](https://github.com/bitcoinj/bitcoinj/blob/d3d5edbcbdb91b25de4df3b6ed6740d7e2329efc/core/src/main/java/org/bitcoinj/script/Script.java#L515:L524)
///
#[inline]
fn decode_from_op_n(op: &All) -> i32 {
    if op.eq(&all::OP_PUSHBYTES_0) {
        0
    } else if op.eq(&all::OP_PUSHNUM_NEG1) {
        -1
    } else {
        op.into_u8() as i32 + 1 - all::OP_PUSHNUM_1.into_u8() as i32
    }
}

///
/// Get number of keys for multisig
///
#[inline]
fn get_num_keys(op: &Instruction) -> Option<i32> {
    match op {
        PushBytes(_) => None,
        Op(op) => {
            if !(op.eq(&all::OP_PUSHNUM_NEG1)
                || op.eq(&all::OP_PUSHBYTES_0)
                || (op.ge(&all::OP_PUSHNUM_1) && all::OP_PUSHNUM_16.ge(op)))
            {
                None
            } else {
                Some(decode_from_op_n(op))
            }
        }
    }
}

///
/// Get address from p2pk script.
///
/// Can only be used for p2pk script,
/// otherwise panic.
///
#[inline]
fn p2pk_to_address(script: &Script) -> Option<Address> {
    assert!(script.is_p2pk());
    if let Some(Ok(Instruction::PushBytes(pk))) = script.instructions().next() {
        // hash the 20 bytes public key
        let pkh = hash160::Hash::hash(pk);
        Some(Address {
            payload: Payload::PubkeyHash(PubkeyHash::from_slice(&pkh).ok()?),
            network: Network::Bitcoin,
        })
    } else {
        unreachable!()
    }
}

trait Cmp {
    fn ge(&self, other: &Self) -> bool;
}

impl Cmp for bitcoin::blockdata::opcodes::All {
    fn ge(&self, other: &Self) -> bool {
        self.into_u8() >= other.into_u8()
    }
}

impl fmt::Display for ScriptType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ScriptType::OpReturn => write!(f, "OpReturn"),
            ScriptType::Pay2MultiSig => write!(f, "Pay2MultiSig"),
            ScriptType::Pay2PublicKey => write!(f, "Pay2PublicKey"),
            ScriptType::Pay2PublicKeyHash => write!(f, "Pay2PublicKeyHash"),
            ScriptType::Pay2ScriptHash => write!(f, "Pay2ScriptHash"),
            ScriptType::Pay2WitnessPublicKeyHash => write!(f, "Pay2WitnessPublicKeyHash"),
            ScriptType::Pay2WitnessScriptHash => write!(f, "Pay2WitnessScriptHash"),
            ScriptType::WitnessProgram => write!(f, "WitnessProgram"),
            ScriptType::Unspendable => write!(f, "Unspendable"),
            ScriptType::NotRecognised => write!(f, "NotRecognised"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_script, ScriptType};
    use bitcoin::hashes::hex::{FromHex, ToHex};
    use bitcoin::{Network, Script};

    #[test]
    fn test_bitcoin_script_p2pkh() {
        // Raw output script: 76a91412ab8dc588ca9d5787dde7eb29569da63c3a238c88ac
        //                    OP_DUP OP_HASH160 OP_PUSHDATA0(20 bytes) 12ab8dc588ca9d5787dde7eb29569da63c3a238c OP_EQUALVERIFY OP_CHECKSIG
        let bytes = [
            0x76 as u8, 0xa9, 0x14, 0x12, 0xab, 0x8d, 0xc5, 0x88, 0xca, 0x9d, 0x57, 0x87, 0xdd,
            0xe7, 0xeb, 0x29, 0x56, 0x9d, 0xa6, 0x3c, 0x3a, 0x23, 0x8c, 0x88, 0xac,
        ];
        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(
            result.addresses.get(0).unwrap().to_string(),
            String::from("12higDjoCCNXSA95xZMWUdPvXNmkAduhWv")
        );
        assert_eq!(result.pattern, ScriptType::Pay2PublicKeyHash);
    }

    #[test]
    fn test_bitcoin_script_p2pk() {
        // https://blockchain.info/tx/e36f06a8dfe44c3d64be2d3fe56c77f91f6a39da4a5ffc086ecb5db9664e8583
        // Raw output script: 0x41 0x044bca633a91de10df85a63d0a24cb09783148fe0e16c92e937fc4491580c860757148effa0595a955f44078b48ba67fa198782e8bb68115da0daa8fde5301f7f9 OP_CHECKSIG
        //                    OP_PUSHDATA0(65 bytes) 0x04bdca... OP_CHECKSIG
        let bytes = [
            0x41 as u8, // Push next 65 bytes
            0x04, 0x4b, 0xca, 0x63, 0x3a, 0x91, 0xde, 0x10, 0xdf, 0x85, 0xa6, 0x3d, 0x0a, 0x24,
            0xcb, 0x09, 0x78, 0x31, 0x48, 0xfe, 0x0e, 0x16, 0xc9, 0x2e, 0x93, 0x7f, 0xc4, 0x49,
            0x15, 0x80, 0xc8, 0x60, 0x75, 0x71, 0x48, 0xef, 0xfa, 0x05, 0x95, 0xa9, 0x55, 0xf4,
            0x40, 0x78, 0xb4, 0x8b, 0xa6, 0x7f, 0xa1, 0x98, 0x78, 0x2e, 0x8b, 0xb6, 0x81, 0x15,
            0xda, 0x0d, 0xaa, 0x8f, 0xde, 0x53, 0x01, 0xf7, 0xf9, 0xac,
        ]; // OP_CHECKSIG
        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(
            result.addresses.get(0).unwrap().to_string(),
            String::from("1LEWwJkDj8xriE87ALzQYcHjTmD8aqDj1f")
        );
        assert_eq!(result.pattern, ScriptType::Pay2PublicKey);
    }

    #[test]
    fn test_bitcoin_script_p2ms() {
        // 2-of-3 Multi sig output
        // OP_2 33 0x022df8750480ad5b26950b25c7ba79d3e37d75f640f8e5d9bcd5b150a0f85014da
        // 33 0x03e3818b65bcc73a7d64064106a859cc1a5a728c4345ff0b641209fba0d90de6e9
        // 33 0x021f2f6e1e50cb6a953935c3601284925decd3fd21bc445712576873fb8c6ebc18 OP_3 OP_CHECKMULTISIG
        let bytes = [
            0x52 as u8, 0x21, 0x02, 0x2d, 0xf8, 0x75, 0x04, 0x80, 0xad, 0x5b, 0x26, 0x95, 0x0b,
            0x25, 0xc7, 0xba, 0x79, 0xd3, 0xe3, 0x7d, 0x75, 0xf6, 0x40, 0xf8, 0xe5, 0xd9, 0xbc,
            0xd5, 0xb1, 0x50, 0xa0, 0xf8, 0x50, 0x14, 0xda, 0x21, 0x03, 0xe3, 0x81, 0x8b, 0x65,
            0xbc, 0xc7, 0x3a, 0x7d, 0x64, 0x06, 0x41, 0x06, 0xa8, 0x59, 0xcc, 0x1a, 0x5a, 0x72,
            0x8c, 0x43, 0x45, 0xff, 0x0b, 0x64, 0x12, 0x09, 0xfb, 0xa0, 0xd9, 0x0d, 0xe6, 0xe9,
            0x21, 0x02, 0x1f, 0x2f, 0x6e, 0x1e, 0x50, 0xcb, 0x6a, 0x95, 0x39, 0x35, 0xc3, 0x60,
            0x12, 0x84, 0x92, 0x5d, 0xec, 0xd3, 0xfd, 0x21, 0xbc, 0x44, 0x57, 0x12, 0x57, 0x68,
            0x73, 0xfb, 0x8c, 0x6e, 0xbc, 0x18, 0x53, 0xae,
        ];

        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(result.pattern, ScriptType::Pay2MultiSig);
    }

    #[test]
    fn test_bitcoin_script_p2sh() {
        // Raw output script: a914e9c3dd0c07aac76179ebc76a6c78d4d67c6c160a
        //                    OP_HASH160 20 0xe9c3dd0c07aac76179ebc76a6c78d4d67c6c160a OP_EQUAL
        let bytes = [
            0xa9 as u8, 0x14, // OP_HASH160, OP_PUSHDATA0(20 bytes)
            0xe9, 0xc3, 0xdd, 0x0c, 0x07, 0xaa, 0xc7, 0x61, 0x79, 0xeb, 0xc7, 0x6a, 0x6c, 0x78,
            0xd4, 0xd6, 0x7c, 0x6c, 0x16, 0x0a, 0x87,
        ]; // OP_EQUAL
        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(
            result.addresses.get(0).unwrap().to_string(),
            String::from("3P14159f73E4gFr7JterCCQh9QjiTjiZrG")
        );
        assert_eq!(result.pattern, ScriptType::Pay2ScriptHash);
    }

    #[test]
    fn test_bitcoin_script_non_standard() {
        // Raw output script: 736372697074
        //                    OP_IFDUP OP_IF OP_2SWAP OP_VERIFY OP_2OVER OP_DEPTH
        let bytes = [0x73 as u8, 0x63, 0x72, 0x69, 0x70, 0x74];
        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(result.addresses.get(0), None);
        assert_eq!(result.pattern, ScriptType::NotRecognised);
    }

    #[test]
    fn test_bitcoin_bogus_script() {
        let bytes = [0x4c as u8, 0xFF, 0x00];
        let result = evaluate_script(
            &Script::from_hex(&bytes.to_hex()).unwrap(),
            Network::Bitcoin,
        );
        assert_eq!(result.addresses.get(0), None);
        assert_eq!(result.pattern, ScriptType::NotRecognised);
    }
}
