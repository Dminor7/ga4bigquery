const getEventParam = (eventParamName, eventParamType = "string", columnName = false) => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    case "float":
      eventParamTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT ep.value.${eventParamTypeName} AS ${eventParamName} FROM UNNEST(event_params) ep WHERE ep.key = '${eventParamName}') AS ${
    columnName ? columnName : eventParamName
  }`;
};
const getEventId = () => {
  return `FARM_FINGERPRINT(CONCAT(event_timestamp, event_name, user_pseudo_id, ifnull((select ep.value.int_value from unnest(event_params) as ep where ep.key = 'engagement_time_msec' ),0))) as event_id`
}

const getSessionId = () => {
return `FARM_FINGERPRINT(CONCAT((select value.int_value from unnest(event_params) where key = 'ga_session_id'), user_pseudo_id)) as session_id`
}

const getDate = () => `DATE(TIMESTAMP_MICROS(event_timestamp), "${constants.TIME_ZONE}") as date`;