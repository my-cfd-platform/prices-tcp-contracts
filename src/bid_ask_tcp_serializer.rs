use my_tcp_sockets::{
    socket_reader::{ReadBuffer, ReadingTcpContractFail, SocketReader},
    TcpSerializationMetadata, TcpSocketSerializer, TcpWriteBuffer,
};

use super::models::BidAskTcpMessage;

static CL_CR: &[u8] = &[13u8, 10u8];

pub struct BidAskTcpSerializer {
    read_buffer: ReadBuffer,
}

impl BidAskTcpSerializer {
    pub fn new() -> Self {
        Self {
            read_buffer: ReadBuffer::new(1024 * 24),
        }
    }
}

#[async_trait::async_trait]
impl TcpSocketSerializer<BidAskTcpMessage, ()> for BidAskTcpSerializer {
    fn serialize(&self, out: &mut impl TcpWriteBuffer, contract: &BidAskTcpMessage, _: &()) {
        contract.serialize(out);
        out.write_slice(CL_CR);
    }

    fn get_ping(&self) -> BidAskTcpMessage {
        return BidAskTcpMessage::Ping;
    }
    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
        _: &(),
    ) -> Result<BidAskTcpMessage, ReadingTcpContractFail> {
        let result = socket_reader
            .read_until_end_marker(&mut self.read_buffer, CL_CR)
            .await?;

        let result = &result[..result.len() - CL_CR.len()];
        let result = BidAskTcpMessage::parse(result);

        match result {
            Ok(result) => Ok(result),
            Err(_) => Err(ReadingTcpContractFail::ErrorReadingSize),
        }
    }
}

impl TcpSerializationMetadata<BidAskTcpMessage> for () {
    fn is_tcp_contract_related_to_metadata(&self, _: &BidAskTcpMessage) -> bool {
        false
    }
    fn apply_tcp_contract(&mut self, _: &BidAskTcpMessage) {}
}
