const helpers = require("./includes/helpers");
const { processingSteps } = require("./includes/processing_steps");
const { recommendedEvents } = require("./includes/recommended_events");
const {
    defaultSessionNames,
    sessionPresets,
    defaultSourceMediumRules,
    defaultPostProcessing,
} = require("./includes/constants");

const bigQueryOptions = [
    "clusterBy",
    "updatePartitionFilter",
    "additionalOptions",
    "partitionExpirationDays",
    "requirePartitionFilter",
];


class DataformAction {
    constructor(sourceConfig, targetConfig) {
        this._source = {
            database: sourceConfig.database || dataform.projectConfig.defaultDatabase,
            dataset: sourceConfig.dataset,
            incrementalTableName: sourceConfig.incrementalTableName,
            incrementalTableEventStepWhere:
                sourceConfig.incrementalTableEventStepWhere || false,
            nonIncrementalTableName:
                sourceConfig.nonIncrementalTableName || "events_*",
            nonIncrementalTableEventStepWhere:
                sourceConfig.nonIncrementalTableEventStepWhere || false,
        };
        this._target = {
            schema:
                targetConfig && targetConfig.schema
                    ? targetConfig.schema
                    : "dataform_staging",
            tableName:
                targetConfig && targetConfig.tableName ? targetConfig.tableName : null,
        };
        this._timezone = "America/Los_Angeles";
        this._tags = [this._source.dataset];
        this._assertions = [];
        this._partitionBy = "date";
        this._uniqueKey = [];
        this._clusterBy = undefined;
        this._updatePartitionFilter = undefined;
        this._additionalOptions = undefined;
        this._partitionExpirationDays = undefined;
        this._requirePartitionFilter = undefined;

        bigQueryOptions.forEach((prop) => {
            this[`_${prop}`] = undefined;
            Object.defineProperty(this, prop, {
                get: function () {
                    return this[`_${prop}`];
                },
                set: function (value) {
                    this[`_${prop}`] = value;
                },
            });
        });
        this._columns = [];
        this._eventParams = [];
        this._userProperties = [];
        this._queryParameters = [];
        this._itemColumns = [];
        this._itemParams = [];
        const properties = [
            "columns",
            "eventParams",
            "userProperties",
            "queryParameters",
            "itemColumns",
            "itemParams",
        ];

        properties.forEach((prop) => {
            Object.defineProperty(this, prop, {
                get: function () {
                    return this[`_${prop}`];
                },
                set: function (value) {
                    this.validateEventParams(value);
                    items.forEach((item) => {
                        if (
                            defaultSessionNames.includes(
                                item.columnName ? item.columnName : item.name
                            )
                        ) {
                            return;
                        }
                        this[`_${prop}`].push(item);
                    });
                },
            });
        });
    }
    #addItemsToProperty(items, propertyName) {
        this.validateEventParams(items);
        const currentItemNames = this[`_${propertyName}`].map((item) => item.name);
        items.forEach((item) => {
            if (
                defaultSessionNames.includes(
                    item.columnName ? item.columnName : item.name
                )
            ) {
                return;
            }
            if (currentItemNames.includes(item.name)) {
                const index = currentItemNames.indexOf(item.name);
                this[`_${propertyName}`][index] = item;
            } else {
                this[`_${propertyName}`].push(item);
            }
            currentItemNames;
        });
    }
    addColumns(columns) {
        this.#addItemsToProperty(columns, "columns");
    }
    addEventParams(eventParams) {
        this.#addItemsToProperty(eventParams, "eventParams");
    }
    addUserProperties(userProperties) {
        this.#addItemsToProperty(userProperties, "userProperties");
    }
    addQueryParameters(queryParameters) {
        this.#addItemsToProperty(queryParameters, "queryParameters");
    }
    addItemColumns(itemColumns) {
        this.#addItemsToProperty(itemColumns, "itemColumns");
    }
    addItemParams(itemParams) {
        this.#addItemsToProperty(itemParams, "itemParams");
    }

    /**
     * Helper method to generate SQL code to get unique id for each row in a final table, used inside publish method. Should be overwritten in the child class.
     */
    getSqlUniqueId() { }

    get timezone() {
        return this._timezone;
    }
    set timezone(timezone) {
        this._timezone = timezone || "America/Los_Angeles";
    }
    get tags() {
        return this._tags;
    }
    set tags(value) {
        if (Array.isArray(value)) {
            this._tags.push(...value);
        } else if (typeof value === "string") {
            this._tags.push(value);
        } else {
            throw new TypeError("Tags should be an array or a string");
        }
    }

    get target() {
        return this._target;
    }
    set target(config) {
        this._target = {
            schema: config.schema || "dataform_staging",
            tableName: config.tableName,
        };
    }

    getConfig() {
        const config = {
            type: "incremental",
            uniqueKey: this._uniqueKey,
            schema: this._target.schema,
            tags: this._tags,
            bigquery: {
                partitionBy: this._partitionBy,
            },
        };
        bigQueryOptions.forEach((prop) => {
            if (this[`_${prop}`]) {
                config.bigquery[prop] = this[`_${prop}`];
            }
        });

        return config;
    }
    get assertions() {
        return this._assertions;
    }
    set assertions(config) {
        if (Array.isArray(value)) {
            this._tags.push(...value);
        } else {
            throw new TypeError("Tags should be an array of assertions");
        }
        this._assertions = config;
    }

    validateEventParams(value) {
        if (!Array.isArray(value)) {
            throw new TypeError("Columns should be an array");
        }

        value.forEach((column) => {
            if (typeof column !== "object" || column === null) {
                throw new TypeError("Each column should be an object");
            }

            // Check if the 'name' property exists and is not an empty string
            if (
                !column.hasOwnProperty("name") ||
                typeof column.name !== "string" ||
                column.name.trim() === ""
            ) {
                throw new TypeError(
                    "Each column should have a required 'name' property of string type and it should not be empty"
                );
            }

            if (
                column.hasOwnProperty("columnName") &&
                typeof column.columnName !== "string"
            ) {
                throw new TypeError(
                    "Each column should have a 'columnName' property of string type"
                );
            }

            const validTypes = [
                "string",
                "int",
                "double",
                "float",
                "coalesce",
                "coalesce_float",
            ];
            if (
                column.hasOwnProperty("type") &&
                !validTypes.includes(column.type.toLowerCase())
            ) {
                throw new TypeError(
                    `Each column should have a 'type' property with one of the following values: ${validTypes.join(
                        ", "
                    )}`
                );
            }
        });
    }
    /**
     * Main method to publish Dataform Action. This method generates SQL and then uses Dataform core [publish](https://cloud.google.com/dataform/docs/reference/dataform-core-reference#publish) method to generate incremental and non-incremental session table. This method should be overwritten in the child class.
     */
    publish() { }

    /**
     * The method to publish default [Dataform assertions](https://cloud.google.com/dataform/docs/assertions). This method should be overwritten in the child class.
     */
    publishAssertions() { }
}


class Session extends DataformAction {
    constructor(sourceConfig, targetConfig) {
        super(sourceConfig, targetConfig);
        this._uniqueKey = ["date", "session_id"];
        this._sourceMediumRules = defaultSourceMediumRules;
        this._postProcessing = defaultPostProcessing;
        this._processingSteps = [...processingSteps];
        this._lastNonDirectLookBackWindow = 30;
        this._target = {
            schema:
                targetConfig && targetConfig.schema
                    ? targetConfig.schema
                    : "dataform_staging",
            tableName:
                targetConfig && targetConfig.tableName
                    ? targetConfig.tableName
                    : "sessions",
        };
        // Apply default presets
        this.applyPreset("standard");
    }
    applyPreset(presetName) {
        if (
            presetName !== "none" &&
            !Object.keys(sessionPresets).includes(presetName)
        ) {
            throw new Error(
                `Invalid sessionPresets name. Possible name are: ${Object.keys(
                    sessionPresets
                ).join(", ")}`
            );
        } else {
            this._columns = [...sessionPresets[presetName]["columns"]];
            this._eventParams = [...sessionPresets[presetName]["eventParams"]];
            this._userProperties = [...sessionPresets[presetName]["userProperties"]];
        }
    }
    getSqlUniqueId() {
        return helpers.getSqlSessionId();
    }
    get lastNonDirectLookBackWindow() {
        return this._lastNonDirectLookBackWindow;
    }
    set lastNonDirectLookBackWindow(lastNonDirectLookBackWindow) {
        this._lastNonDirectLookBackWindow = lastNonDirectLookBackWindow || 30;
    }
    get sourceMediumRules() {
        return this._sourceMediumRules;
    }
    set sourceMediumRules(config) {
        this._sourceMediumRules = config;
    }
    get processingSteps() {
        return this._processingSteps;
    }
    set processingSteps(config) {
        this._processingSteps = config;
    }
    get postProcessing() {
        return this._postProcessing;
    }
    set postProcessing(config) {
        this._postProcessing = config;
    }
    skipLastNonDirectStep() {
        this._processingSteps = this._processingSteps.filter(
            (step) => step.queryName !== "sessions_with_last_non_direct"
        );
    }
    skipChannelStep() {
        if (
            this._processingSteps.filter(
                (step) => step.queryName == "sessions_with_last_non_direct"
            ).length > 0
        ) {
            throw new Error(
                `Last direct step can't be skipped as it's required by following steps. Please delete last direct attribution step first.`
            );
        }
        this._processingSteps = this._processingSteps.filter(
            (step) => step.queryName !== "sessions_with_channel"
        );
    }
    skipSourceMediumStep() {
        if (
            this._processingSteps.filter(
                (step) => step.queryName == "sessions_with_channel"
            ).length > 0
        ) {
            throw new Error(
                `Last direct step can't be skipped as it's required by following steps. Please delete channel step first.`
            );
        }
        if (
            this._processingSteps.filter(
                (step) => step.queryName == "sessions_with_last_non_direct"
            ).length > 0
        ) {
            throw new Error(
                `Last direct step can't be skipped as it's required by following steps. Please delete last direct attribution step first.`
            );
        }

        this._processingSteps = this._processingSteps.filter(
            (step) => step.queryName !== "sessions_with_source_medium_and_lp"
        );
    }
    publish() {
        if (!this._target.tableName) {
            throw new Error("Table name is required, please set target.tableName");
        }
        const sessions = publish(this._target.tableName, {
            schema: this._target.schema,
        })
            .config(this.getConfig())
            .query(
                (ctx) => `
          
          -- prepare events table
          with events as (
            select
              ${this.getSqlUniqueId()},
              ${helpers.getSqlDate(this._timezone)},
              TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
              user_id,
              user_pseudo_id,
              COALESCE(
                  (SELECT value.int_value FROM unnest(event_params) WHERE key = "session_engaged"),
                  (CASE WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" THEN 1 END)
              ) as session_engaged,            
              ${helpers.getSqlEventParam("page_location")},
              ${helpers.getSqlEventParam("page_referrer")},
              ${helpers.getSqlEventParam("ignore_referrer")},
              ${helpers.getSqlEventParam("source")},
              ${helpers.getSqlEventParam("medium")},
              ${helpers.getSqlEventParam("campaign")},
              ${helpers.getSqlEventParam("gclid")},
              ${ctx.when(
                    this._columns.length > 0,
                    `${helpers.getSqlColumns(this._columns)}, `
                )}
              ${ctx.when(
                    this._eventParams.length > 0,
                    `${helpers.getSqlEventParams(this._eventParams)}, `
                )}
              ${ctx.when(
                    this._userProperties.length > 0,
                    `${helpers.getSqlUserProperties(this._userProperties)}, `
                )}
              ${ctx.when(
                    this._queryParameters.length > 0,
                    `${helpers.getSqlQueryParameters(
                        helpers.getSqlEventParam("page_location", "string", null),
                        this._queryParameters
                    )}, `
                )}
  
            from
            ${ctx.when(
                    ctx.incremental(),
                    `${ctx.ref(
                        this._source.database,
                        this._source.dataset,
                        this._source.incrementalTableName
                    )}
              ${this._source.incrementalTableEventStepWhere
                        ? "WHERE " + this._source.incrementalTableEventStepWhere
                        : ""
                    }
              `
                )}
            ${ctx.when(
                    !ctx.incremental(),
                    `${ctx.ref(
                        this._source.database,
                        this._source.dataset,
                        this._source.nonIncrementalTableName
                    )}
              ${this._source.nonIncrementalTableEventStepWhere
                        ? "WHERE " + this._source.nonIncrementalTableEventStepWhere
                        : ""
                    }
              `
                )}
  
  
          )
          
          -- add source_categories table if channel step is required
          ${this._processingSteps
                        .map((step) => step.queryName)
                        .includes("sessions_with_channel")
                        ? `, source_categories as (
                      ${helpers.getSqlSourceCategoriesTable()}
                  )`
                        : ""
                    }
  
          -- add processing steps
          ${this._processingSteps.length > 0
                        ? "," +
                        this._processingSteps
                            .map((processingStep) => processingStep.query(this, ctx))
                            .join(",\n")
                        : ""
                    }
         
          -- select final result from the last processing step and apply post processing
          select
          * ${this._postProcessing &&
                        this._postProcessing.delete &&
                        this._postProcessing.delete.length > 0
                        ? `except (${this._postProcessing.delete.join(", ")})`
                        : ""
                    }
          from ${this._processingSteps.length > 0
                        ? this._processingSteps[this._processingSteps.length - 1].queryName
                        : "events"
                    }
    `
            );

        return sessions;
    }

}


module.exports = {
    declareSources: helpers.declareSources,
    Session
  };