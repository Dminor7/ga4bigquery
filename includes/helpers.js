const getSqlEventParam = (
  paramName,
  paramType = "string",
  columnName = false
) => getSqlUnnestParam(paramName, paramType, columnName);

const getSqlUserProperty = (
  paramName,
  paramType = "string",
  columnName = false
) => getSqlUnnestParam(paramName, paramType, columnName, "user_properties");

const getSqlUnnestParam = (
  paramName,
  paramType = "string",
  resultColumnName = false,
  unnestColumnName = "event_params"
) => {
  let paramTypeName;
  const alias =
    resultColumnName === null
      ? ""
      : `AS ${resultColumnName ? resultColumnName : paramName} `;
  if (paramType.toLowerCase() === "coalesce") {
    return `(SELECT COALESCE(ep.value.string_value, SAFE_CAST(ep.value.int_value AS STRING), SAFE_CAST(ep.value.double_value AS STRING), SAFE_CAST(ep.value.float_value AS STRING)) FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}' LIMIT 1) ${alias}`;
  }
  if (paramType.toLowerCase() === "coalesce_float") {
    return `(SELECT COALESCE(ep.value.float_value, SAFE_CAST(ep.value.int_value AS FLOAT64), ep.value.double_value) FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}' LIMIT 1) ${alias}`;
  }
  switch (paramType.toLowerCase()) {
    case "string":
      paramTypeName = "string_value";
      break;
    case "int":
      paramTypeName = "int_value";
      break;
    case "double":
      paramTypeName = "double_value";
      break;
    case "float":
      paramTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT ep.value.${paramTypeName} FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}' LIMIT 1) ${alias}`;
};

const getSqlEventParams = (eventParams) => {
  const sql = eventParams.map((eventParam) =>
    getSqlEventParam(eventParam.name, eventParam.type, eventParam.columnName)
  );
  return eventParams.length > 0 ? sql.join(", ") : "";
};

const getSqlUserProperties = (userProperties) => {
  const sql = userProperties.map((userProperty) =>
    getSqlUserProperty(
      userProperty.name,
      userProperty.type,
      userProperty.columnName
    )
  );
  return userProperties.length > 0 ? sql.join(", ") : "";
};

const getSqlGetFirstNotNullValue = (
  paramName,
  columnName = false,
  orderBy = "event_timestamp"
) => {
  const alias =
    columnName === null ? "" : `AS ${columnName ? columnName : paramName} `;
  return `ARRAY_AGG(${paramName} IGNORE NULLS ORDER BY ${orderBy} LIMIT 1)[SAFE_OFFSET(0)] ${alias}`;
};

const getSqlGetFirstNotNullValues = (columns) => {
  const sql = columns.map((column) =>
    getSqlGetFirstNotNullValue(
      column.columnName ? column.columnName : column.name
    )
  );
  return columns.length > 0 ? sql.join(",") : "";
};

const getSqlColumns = (params) => {
  const sql = params.map(
    (param) =>
      `${param.name} as ${param.columnName ? param.columnName : param.name}`
  );
  return params.length > 0 ? sql.join(", ") : "";
};

const getSqlQueryParameter = (url, param) => {
  const sql = `REGEXP_EXTRACT(${url}, r'(?i).*[?&#]${param.name.toLowerCase()}=([^&#\?]*)') as ${param.columnName ? param.columnName : param.name
    }`;
  return sql;
};

const getSqlQueryParameters = (url, params) => {
  const sql = params.map((param) => getSqlQueryParameter(url, param));
  return params.length > 0 ? sql.join(", ") : "";
};

const getDateFromTableName = (tblName) => {
  return tblName.substring(7);
};

const getFormattedDateFromTableName = (tblName) => {
  return `${tblName.substring(7, 11)}-${tblName.substring(
    11,
    13
  )}-${tblName.substring(13)}`;
};

const getSqlList = (list) => {
  return `('${list.join("','")}')`;
};
const getSqlEventId = (timestampEventParamName) => {
  if (typeof timestampEventParamName === "undefined")
    return `FARM_FINGERPRINT(CONCAT(event_timestamp, event_name, user_pseudo_id, ifnull((select ep.value.int_value from unnest(event_params) as ep where ep.key = 'engagement_time_msec' ),0))) as event_id`;
  return `FARM_FINGERPRINT(CONCAT(ifnull((SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = '${timestampEventParamName}'),event_timestamp), event_name, user_pseudo_id)) as event_id`;
};

const getSqlSessionId = () => {
  return `FARM_FINGERPRINT(CONCAT((select value.int_value from unnest(event_params) where key = 'ga_session_id'), user_pseudo_id)) as session_id`;
};

const getSqlDate = (timezone = "America/Los_Angeles") =>
  `DATE(TIMESTAMP_MICROS(event_timestamp), "${timezone}") as date`;

function isStringInteger(str) {
  const num = Number(str);
  return Number.isInteger(num);
}

const getSqlSelectFromRow = (config) => {
  return Object.entries(config)
    .map(([key, value]) => {
      if (typeof value === "number") {
        return `${value} AS ${key}`;
      } else if (key === "date") {
        return `DATE '${value}' AS ${key}`;
      } else if (key === "event_timestamp" && !/^\d+$/.test(value)) {
        return `TIMESTAMP '${value}' AS ${key}`;
      } else if (key === "session_start" && !/^\d+$/.test(value)) {
        return `TIMESTAMP '${value}' AS ${key}`;
      } else if (key === "session_end" && !/^\d+$/.test(value)) {
        return `TIMESTAMP '${value}' AS ${key}`;
      } else if (typeof value === "string") {
        if (key === "int_value") return `${parseInt(value)} AS ${key}`;
        if (key.indexOf("timestamp") > -1)
          return `${parseInt(value)} AS ${key}`;
        if (key === "float_value" || key === "double_value")
          return `${parseFloat(value)} AS ${key}`;
        return `'${value}' AS ${key}`;
      } else if (value === null) {
        return `${value} AS ${key}`;
      } else if (value instanceof Array) {
        return `[${getSqlSelectFromRow(value)}] AS ${key}`;
      } else {
        if (isStringInteger(key))
          return `STRUCT(${getSqlSelectFromRow(value)})`;
        else return `STRUCT(${getSqlSelectFromRow(value)}) AS ${key}`;
      }
    })
    .join(", ");
};

const getSqlUnionAllFromRows = (rows) => {
  try {
    const selectStatements = rows
      .map((data) => "SELECT " + getSqlSelectFromRow(data))
      .join("\nUNION ALL\n ");
    return selectStatements;
  } catch (err) {
    console.error("Error reading or parsing the file", err);
  }
};


function getSqlSourceMediumCaseStatement(config) {
  const conditions = config
    .map((condition) => {
      return `WHEN ${getSqlCondition(condition)} THEN struct(${condition.value.source
        } as source, ${condition.value.medium} as medium, ${condition.value.campaign
        } as campaign
      )`;
    })
    .join(" ");

  return `CASE ${conditions} ELSE struct('(direct)' as source, '(none)' as medium, '(not set)' as campaign) END`;
}

function getSqlCondition(condition) {
  let sql = "";
  let column;
  if (condition.columns.length > 1) {
    column = `coalesce(${condition.columns.join(", ")})`;
  } else {
    column = condition.columns[0];
  }

  switch (condition.conditionType) {
    case "NOT_NULL":
      sql = `${column} IS NOT NULL`;
      break;
    case "REGEXP_CONTAINS":
      sql = `REGEXP_CONTAINS(${column}, r'${condition.conditionValue}')`;
      break;
    default:
      throw new TypeError(
        "Unsupported condition type. Supported types are: NOT_NULL"
      );
      break;
  }
  return sql;
}

function getSqlSourceCategoriesTable() {
  const { rows } = require("./ga4_source_categories.js");
  return getSqlUnionAllFromRows(rows);
}

function getSqlDefaultChannelGrouping(source, medium, source_category) {
  return `
    case 
    when 
        (
        ${source} is null 
            and ${medium} is null
        )
        or (
        ${source} = '(direct)'
        and (${medium} = '(none)' or ${medium} = '(not set)')
        ) 
        then 'Direct'
    when 
        (
        REGEXP_CONTAINS(${source}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
        or ${source_category} = 'SOURCE_CATEGORY_SOCIAL' 
        )
        and REGEXP_CONTAINS(${medium}, r"^(.*cp.*|ppc|retargeting|paid.*)$") = true
        then 'Paid Social'
    when 
        REGEXP_CONTAINS(${source}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
        or ${medium} in ("social","social-network","social-media","sm","social network","social media")
        or ${source_category} = 'SOURCE_CATEGORY_SOCIAL' 
        then 'Organic Social'
    when 
        REGEXP_CONTAINS(${medium}, r"email|e-mail|e_mail|e mail") = true
        or REGEXP_CONTAINS(${source}, r"email|e-mail|e_mail|e mail") = true
        then 'Email'
    when 
        REGEXP_CONTAINS(${medium}, r"affiliate|affiliates") = true
        then 'Affiliates'
    when 
        ${source_category} = 'SOURCE_CATEGORY_SHOPPING' 
        and REGEXP_CONTAINS(${medium},r"^(.*cp.*|ppc|paid.*)$")
        then 'Paid Shopping'
    when 
        (${source_category} = 'SOURCE_CATEGORY_VIDEO' AND REGEXP_CONTAINS(${medium},r"^(.*cp.*|ppc|paid.*)$"))
        or ${source} = 'dv360_video'
        then 'Paid Video'
    when 
        REGEXP_CONTAINS(${medium}, r"^(display|cpm|banner)$")
        or ${source} = 'dv360_display'
        then 'Display'
    when 
        ${source_category} = 'SOURCE_CATEGORY_SEARCH'
        and REGEXP_CONTAINS(${medium}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
        then 'Paid Search'
    when 
        REGEXP_CONTAINS(${medium}, r"^(cpv|cpa|cpp|content-text)$")
        then 'Other Advertising'
    when 
        ${medium} = 'organic' or ${source_category} = 'SOURCE_CATEGORY_SEARCH'
        then 'Organic Search'
    when 
        ${source_category} = 'SOURCE_CATEGORY_VIDEO'
        or REGEXP_CONTAINS(${medium}, r"^(.*video.*)$")
        then 'Organic Video'
    when 
        ${source_category} = 'SOURCE_CATEGORY_SHOPPING'
        then 'Organic Shopping'
    when 
        ${medium} in ("referral", "app", "link")
        then 'Referral'
    when 
        ${medium} = 'audio'
        then 'Audio'
    when 
        ${medium} = 'sms'
        or ${source} = 'sms'
        then 'SMS'
    when 
        REGEXP_CONTAINS(${medium}, r"(mobile|notification|push$)") or ${source} = 'firebase'
        then 'Push Notifications'
    else '(Other)' 
    end 
    `;
}

const declareSources = ({
  database = dataform.projectConfig.defaultDatabase,
  dataset,
  incrementalTableName,
  nonIncrementalTableName = "events_*",
}) => {
  declare({
    database,
    schema: dataset,
    name: incrementalTableName,
  });
  if (incrementalTableName != nonIncrementalTableName) {
    declare({
      database,
      schema: dataset,
      name: nonIncrementalTableName,
    });
  }
};

module.exports = {
  getDateFromTableName,
  getFormattedDateFromTableName,
  getSqlUnnestParam,
  getSqlEventParam,
  getSqlEventParams,
  getSqlUserProperty,
  getSqlUserProperties,
  getSqlList,
  getSqlEventId,
  getSqlSessionId,
  getSqlDate,
  getSqlGetFirstNotNullValue,
  getSqlGetFirstNotNullValues,
  getSqlColumns,
  getSqlQueryParameter,
  getSqlQueryParameters,
  getSqlUnionAllFromRows,
  getSqlSourceMediumCaseStatement,
  getSqlDefaultChannelGrouping,
  getSqlSourceCategoriesTable,
  declareSources,
};