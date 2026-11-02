#[derive(prost::Message, sark_gen::Message)]
#[prost(skip_debug)]
pub struct S {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}

#[derive(prost::Message, sark_gen::Message)]
#[prost(skip_debug)]
pub struct P {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}

#[derive(prost::Message, sark_gen::Message)]
#[prost(skip_debug)]
pub struct W {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}
