use ethers_core::types::{Address, H256, U256};

pub mod block;
pub mod event;
pub mod uniswap_v2;
pub mod uniswap_v3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Value(pub H256);
impl Value {
  pub fn as_address(&self) -> anyhow::Result<Address> {
    if self.0[..12] != [0; 12] {
      return Err(anyhow::anyhow!("Invalid address"));
    }
    Ok(Address::from_slice(&self.0[12..]))
  }

  pub fn as_u256(&self) -> U256 {
    U256::from_big_endian(self.0.as_bytes())
  }

  pub fn as_u32(&self) -> u32 {
    self.as_u256().as_u32()
  }

  pub fn as_u64(&self) -> u64 {
    self.as_u256().as_u64()
  }

  pub fn as_u128(&self) -> u128 {
    self.as_u256().as_u128()
  }
}

pub trait ToHex {
  fn to_hex(&self) -> String;
}
pub trait ToChecksumHex {
  fn to_checksum_hex(&self) -> String;
}

impl ToChecksumHex for Address {
  fn to_checksum_hex(&self) -> String {
    ethers_core::utils::to_checksum(self, None)
  }
}

macro_rules! impl_to_hex {
  ($ty:ty) => {
    impl ToHex for $ty {
      fn to_hex(&self) -> String {
        if self.is_zero() {
          "0x0".to_string()
        } else {
          format!("0x{:x}", self)
        }
      }
    }
  };
}
// impl_to_hex!(ethers_core::types::H160);
impl_to_hex!(ethers_core::types::H256);

#[test]
fn test_to_hex() {
  use ethers_core::types::{Address, H256};
  assert_eq!(H256::zero().to_hex(), "0x0");
  assert_eq!(Address::from_low_u64_be(1).to_checksum_hex(), "0x0000000000000000000000000000000000000001");
}
