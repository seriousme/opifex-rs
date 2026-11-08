use std::cell::RefCell;
use std::rc::Rc;

use opifex_protocol::{
    ConnectPacket, MqttPacket, ProtocolLevel, PublishPacket, QoS, SubscribePacket,
};
use wasm_bindgen::prelude::*;
use web_sys::{BinaryType, MessageEvent, WebSocket};

#[wasm_bindgen]
pub struct BrowserClient {
    url: String,
    client_id: String,
    protocol_level: ProtocolLevel,
    socket: Rc<RefCell<Option<WebSocket>>>,
    on_message: Rc<RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>>,
}

#[wasm_bindgen]
impl BrowserClient {
    #[wasm_bindgen(constructor)]
    pub fn new(url: &str, client_id: &str) -> Self {
        Self {
            url: url.to_string(),
            client_id: client_id.to_string(),
            protocol_level: ProtocolLevel::V5,
            socket: Rc::new(RefCell::new(None)),
            on_message: Rc::new(RefCell::new(None)),
        }
    }

    #[wasm_bindgen(js_name = setProtocolLevel)]
    pub fn set_protocol_level(&mut self, protocol_level: u8) -> Result<(), JsValue> {
        self.protocol_level = ProtocolLevel::try_from(protocol_level)
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen(getter)]
    pub fn connected(&self) -> bool {
        self.socket
            .borrow()
            .as_ref()
            .map(|socket| socket.ready_state() == WebSocket::OPEN)
            .unwrap_or(false)
    }

    #[wasm_bindgen(js_name = connect)]
    pub fn connect(&self) -> Result<(), JsValue> {
        let socket = WebSocket::new(&self.url)?;
        socket.set_binary_type(BinaryType::Arraybuffer);

        let message_handler: Closure<dyn FnMut(MessageEvent)> =
            Closure::new(|_event: MessageEvent| {
                // The browser transport owns the received bytes; callers can attach
                // a JavaScript listener if they want to decode MQTT frames on the
                // application side. This binding intentionally keeps the transport
                // surface small and browser-native.
            });

        socket.set_onmessage(Some(message_handler.as_ref().unchecked_ref()));
        *self.socket.borrow_mut() = Some(socket);
        *self.on_message.borrow_mut() = Some(message_handler);

        let packet = ConnectPacket {
            protocol_level: self.protocol_level,
            client_id: self.client_id.clone(),
            clean_session: true,
            keep_alive: 60,
            username: None,
            password: None,
            will: None,
        };
        self.send_packet(
            &packet
                .encode()
                .map_err(|err| JsValue::from_str(&err.to_string()))?,
        )
    }

    #[wasm_bindgen(js_name = publish)]
    pub fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> Result<(), JsValue> {
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(JsValue::from_str(
                    "unsupported QoS value; expected 0, 1, or 2",
                ))
            }
        };

        let packet = PublishPacket {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            packet_id: None,
            payload: payload.to_vec(),
        };
        self.send_packet(
            &packet
                .encode()
                .map_err(|err| JsValue::from_str(&err.to_string()))?,
        )
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(&self, topic: &str, qos: u8) -> Result<(), JsValue> {
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(JsValue::from_str(
                    "unsupported QoS value; expected 0, 1, or 2",
                ))
            }
        };

        let packet = SubscribePacket {
            packet_id: 1,
            subscriptions: vec![(topic.to_string(), qos)],
        };
        self.send_packet(
            &packet
                .encode()
                .map_err(|err| JsValue::from_str(&err.to_string()))?,
        )
    }

    #[wasm_bindgen(js_name = ping)]
    pub fn ping(&self) -> Result<(), JsValue> {
        let packet = MqttPacket::PingReq;
        self.send_packet(
            &packet
                .encode()
                .map_err(|err| JsValue::from_str(&err.to_string()))?,
        )
    }

    #[wasm_bindgen(js_name = disconnect)]
    pub fn disconnect(&self) -> Result<(), JsValue> {
        let packet = MqttPacket::Disconnect;
        let result = self.send_packet(
            &packet
                .encode()
                .map_err(|err| JsValue::from_str(&err.to_string()))?,
        );
        if let Some(socket) = self.socket.borrow().as_ref() {
            let _ = socket.close();
        }
        result
    }

    fn send_packet(&self, packet: &[u8]) -> Result<(), JsValue> {
        let socket = self
            .socket
            .borrow()
            .as_ref()
            .ok_or_else(|| JsValue::from_str("WebSocket is not connected"))?
            .clone();
        socket.send_with_u8_array(packet).map_err(|err| err.into())
    }
}

#[wasm_bindgen(start)]
pub fn main() {
    let _ = BrowserClient::new("ws://127.0.0.1:1883/mqtt", "browser-client");
}
