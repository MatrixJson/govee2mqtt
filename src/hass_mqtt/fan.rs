use crate::hass_mqtt::base::{Device, EntityConfig, Origin};
use crate::hass_mqtt::instance::{publish_entity_config, EntityInstance};
use crate::platform_api::DeviceParameters;
use crate::service::device::Device as ServiceDevice;
use crate::service::hass::{availability_topic, topic_safe_id, HassClient, IdParameter};
use crate::service::state::StateHandle;
use anyhow::anyhow;
use async_trait::async_trait;
use mosquitto_rs::router::{Params, Payload, State};
use serde::Serialize;

/// Govee describes the fan portion of a device through these capability
/// instances. They are consistent across the ceiling fan line, so we key off
/// the capabilities rather than off the SKU. Note that Govee reports these
/// devices as `devices.types.light`, so the device type tells us nothing.
const FAN_TOGGLE: &str = "fanToggle";
const FAN_SPEED_MODE: &str = "fanSpeedMode";
const REVERSE_AIRFLOW_TOGGLE: &str = "reverseAirflowToggle";

/// <https://www.home-assistant.io/integrations/fan.mqtt>
#[derive(Serialize, Clone, Debug)]
pub struct FanConfig {
    #[serde(flatten)]
    pub base: EntityConfig,

    /// HASS publishes ON/OFF here to start and stop the fan
    pub command_topic: String,

    /// HASS publishes the speed here, as a value in
    /// `speed_range_min..=speed_range_max`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percentage_command_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_range_min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_range_max: Option<i64>,

    /// HASS publishes `forward` or `reverse` here
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction_command_topic: Option<String>,

    /// Govee returns an empty string for the state of every fan capability,
    /// even immediately after a control call it reported as successful, so
    /// there is nothing for us to publish back. Run the entity optimistically
    /// and let HASS track what it last asked for.
    /// <https://developer.govee.com/discuss/6596e84c901fb900312d5968>
    pub optimistic: bool,
}

pub struct Fan {
    fan: FanConfig,
}

impl Fan {
    /// Build a fan entity for devices that expose a fan, or None for
    /// everything else.
    pub fn for_device(device: &ServiceDevice) -> Option<Self> {
        // Without an on/off toggle there is no fan to speak of, and none of
        // the other controls are reachable.
        device.get_capability_by_instance(FAN_TOGGLE)?;

        let id = topic_safe_id(device);

        let (speed_range_min, speed_range_max, percentage_command_topic) = match speed_range(device)
        {
            Some((min, max)) => (
                Some(min),
                Some(max),
                Some(format!("gv2mqtt/fan/{id}/set-percentage")),
            ),
            None => (None, None, None),
        };

        let direction_command_topic = device
            .get_capability_by_instance(REVERSE_AIRFLOW_TOGGLE)
            .map(|_| format!("gv2mqtt/fan/{id}/set-direction"));

        Some(Self {
            fan: FanConfig {
                base: EntityConfig {
                    availability_topic: availability_topic(),
                    // No name, so that hass uses the device name and this
                    // becomes the primary fan entity for the device.
                    name: None,
                    device_class: None,
                    origin: Origin::default(),
                    device: Device::for_device(device),
                    unique_id: format!("gv2mqtt-{id}-fan"),
                    entity_category: None,
                    icon: None,
                },
                command_topic: format!("gv2mqtt/fan/{id}/command"),
                percentage_command_topic,
                speed_range_min,
                speed_range_max,
                direction_command_topic,
                optimistic: true,
            },
        })
    }
}

/// Resolve the lowest and highest speed that the device accepts for
/// `fanSpeedMode`. Govee reports these as an enum of named options such as
/// `Speed 1` through `Speed 6`; hass wants the numeric bounds so that it can
/// map its percentage slider onto them.
fn speed_range(device: &ServiceDevice) -> Option<(i64, i64)> {
    let cap = device.get_capability_by_instance(FAN_SPEED_MODE)?;
    let DeviceParameters::Enum { options } = cap.parameters.as_ref()? else {
        return None;
    };

    let mut min = None;
    let mut max = None;
    for value in options.iter().filter_map(|opt| opt.value.as_i64()) {
        min = Some(min.map_or(value, |m: i64| m.min(value)));
        max = Some(max.map_or(value, |m: i64| m.max(value)));
    }

    match (min, max) {
        // A single speed is not a range worth showing a slider for
        (Some(min), Some(max)) if min < max => Some((min, max)),
        _ => None,
    }
}

/// Govee ships the fan speeds of a ceiling fan in the same scene list that it
/// uses for light effects, so `Speed 1` through `Speed 6` turn up sorted in
/// among the real effects on the device's light entity. Drop any scene whose
/// name matches one of the device's own `fanSpeedMode` options.
///
/// Matching against the options the device reports, rather than against a
/// pattern like "Speed N", means a device with no fan keeps every scene it
/// reports, even one genuinely named `Speed 1`.
pub fn filter_fan_speed_scenes(device: &ServiceDevice, scenes: Vec<String>) -> Vec<String> {
    let Some(cap) = device.get_capability_by_instance(FAN_SPEED_MODE) else {
        return scenes;
    };
    let Some(DeviceParameters::Enum { options }) = &cap.parameters else {
        return scenes;
    };

    scenes
        .into_iter()
        .filter(|name| {
            !options
                .iter()
                .any(|opt| opt.name.eq_ignore_ascii_case(name))
        })
        .collect()
}

#[async_trait]
impl EntityInstance for Fan {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        publish_entity_config("fan", state, client, &self.fan.base, &self.fan).await
    }

    async fn notify_state(&self, _client: &HassClient) -> anyhow::Result<()> {
        // Nothing to report: see the comment on FanConfig::optimistic
        Ok(())
    }
}

/// Set one of the device's on/off toggles through the platform API
async fn set_toggle(
    state: &StateHandle,
    device: &ServiceDevice,
    instance: &str,
    on: bool,
) -> anyhow::Result<()> {
    let client = state
        .get_platform_client()
        .await
        .ok_or_else(|| anyhow!("no platform API client available to set {instance}"))?;
    let info = device
        .http_device_info
        .as_ref()
        .ok_or_else(|| anyhow!("no platform metadata for {device}, cannot set {instance}"))?;

    client.set_toggle_state(info, instance, on).await?;
    Ok(())
}

pub async fn mqtt_fan_command(
    Payload(command): Payload<String>,
    Params(IdParameter { id }): Params<IdParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    log::info!("mqtt_fan_command: {id}: {command}");
    let device = state.resolve_device_for_control(&id).await?;

    let on = match command.as_str() {
        "ON" | "on" => true,
        "OFF" | "off" => false,
        _ => anyhow::bail!("invalid fan command {command} for {id}"),
    };

    set_toggle(&state, &device, FAN_TOGGLE, on).await
}

pub async fn mqtt_fan_set_percentage(
    Payload(speed): Payload<i64>,
    Params(IdParameter { id }): Params<IdParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    log::info!("mqtt_fan_set_percentage: {id}: {speed}");
    let device = state.resolve_device_for_control(&id).await?;

    // hass sends a value in speed_range_min..=speed_range_max, but it uses
    // zero to mean off, which is outside that range.
    if speed == 0 {
        return set_toggle(&state, &device, FAN_TOGGLE, false).await;
    }

    let cap = device
        .get_capability_by_instance(FAN_SPEED_MODE)
        .ok_or_else(|| anyhow!("{device} has no {FAN_SPEED_MODE}"))?
        .clone();

    state.device_control(&device, &cap, speed).await?;

    // Changing the speed does not start the fan, so make sure it is running.
    set_toggle(&state, &device, FAN_TOGGLE, true).await
}

pub async fn mqtt_fan_set_direction(
    Payload(direction): Payload<String>,
    Params(IdParameter { id }): Params<IdParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    log::info!("mqtt_fan_set_direction: {id}: {direction}");
    let device = state.resolve_device_for_control(&id).await?;

    let reverse = match direction.as_str() {
        "forward" => false,
        "reverse" => true,
        _ => anyhow::bail!("invalid fan direction {direction} for {id}"),
    };

    set_toggle(&state, &device, REVERSE_AIRFLOW_TOGGLE, reverse).await
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::platform_api::HttpDeviceInfo;

    fn device_with_capabilities(capabilities: &str) -> ServiceDevice {
        let info: HttpDeviceInfo = serde_json::from_str(&format!(
            r#"{{
                "sku": "H1310",
                "device": "AA:BB:CC:DD",
                "deviceName": "Test Fan",
                "type": "devices.types.light",
                "capabilities": [{capabilities}]
            }}"#
        ))
        .unwrap();

        let mut device = ServiceDevice::new("H1310", "AA:BB:CC:DD");
        device.set_http_device_info(info);
        device
    }

    const FAN_CAPS: &str = r#"
        {
            "type": "devices.capabilities.toggle",
            "instance": "fanToggle",
            "parameters": {
                "dataType": "ENUM",
                "options": [{"name": "on", "value": 1}, {"name": "off", "value": 0}]
            }
        },
        {
            "type": "devices.capabilities.mode",
            "instance": "fanSpeedMode",
            "parameters": {
                "dataType": "ENUM",
                "options": [
                    {"name": "Speed 1", "value": 1},
                    {"name": "Speed 2", "value": 2},
                    {"name": "Speed 3", "value": 3}
                ]
            }
        }
    "#;

    const LIGHT_ONLY_CAPS: &str = r#"
        {
            "type": "devices.capabilities.color_setting",
            "instance": "colorRgb",
            "parameters": {
                "dataType": "INTEGER",
                "range": {"min": 0, "max": 16777215, "precision": 1}
            }
        }
    "#;

    #[test]
    fn speed_range_spans_the_reported_options() {
        assert_eq!(
            speed_range(&device_with_capabilities(FAN_CAPS)),
            Some((1, 3))
        );
    }

    #[test]
    fn a_device_without_a_fan_has_no_speed_range() {
        assert_eq!(
            speed_range(&device_with_capabilities(LIGHT_ONLY_CAPS)),
            None
        );
    }

    #[test]
    fn fan_entity_is_built_only_for_devices_with_a_fan() {
        assert!(Fan::for_device(&device_with_capabilities(FAN_CAPS)).is_some());
        assert!(Fan::for_device(&device_with_capabilities(LIGHT_ONLY_CAPS)).is_none());
    }

    #[test]
    fn fan_speeds_are_dropped_from_the_scene_list() {
        let scenes = vec![
            "Sunrise".to_string(),
            "Speed 1".to_string(),
            "Speed 2".to_string(),
            "Speed 3".to_string(),
            "Sunset".to_string(),
        ];

        assert_eq!(
            filter_fan_speed_scenes(&device_with_capabilities(FAN_CAPS), scenes),
            vec!["Sunrise".to_string(), "Sunset".to_string()]
        );
    }

    #[test]
    fn a_device_without_a_fan_keeps_a_scene_named_like_a_speed() {
        let scenes = vec!["Speed 1".to_string(), "Sunrise".to_string()];

        assert_eq!(
            filter_fan_speed_scenes(&device_with_capabilities(LIGHT_ONLY_CAPS), scenes.clone()),
            scenes
        );
    }
}
