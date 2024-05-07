use my_tcp_sockets::{
    socket_reader::{ReadBuffer, ReadingTcpContractFail, SocketReader},
    TcpSerializerFactory, TcpSerializerState, TcpSocketSerializer, TcpWriteBuffer,
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

impl Default for BidAskTcpSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TcpSocketSerializer<BidAskTcpMessage, ()> for BidAskTcpSerializer {
    // const PING_PACKET_IS_SINGLETONE: bool = false;

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
    // fn serialize_ref(&self, contract: &TContract) -> Vec<u8>{
    //     return vec![];
    // }
    // fn apply_packet(&mut self, contract: &TContract) -> bool{
    //     false
    // }
}

pub struct TcpFeedSerializerFactory;

#[async_trait::async_trait]
impl TcpSerializerFactory<BidAskTcpMessage, BidAskTcpSerializer, ()> for TcpFeedSerializerFactory {
    async fn create_serializer(&self) -> BidAskTcpSerializer {
        BidAskTcpSerializer::new()
    }
    async fn create_serializer_state(&self) -> () {
        ()
    }
}

impl TcpSerializerState<BidAskTcpMessage> for () {
    fn is_tcp_contract_related_to_metadata(&self, _contract: &BidAskTcpMessage) -> bool {
        false
    }
    fn apply_tcp_contract(&mut self, _contract: &BidAskTcpMessage) {}
}
