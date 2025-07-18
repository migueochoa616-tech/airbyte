/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.cdc.AbortDebeziumWarmStartState
import io.airbyte.cdk.read.cdc.CdcPartitionReaderDebeziumOperations
import io.airbyte.cdk.read.cdc.CdcPartitionsCreatorDebeziumOperations
import io.airbyte.cdk.read.cdc.DebeziumOffset
import io.airbyte.cdk.read.cdc.DebeziumRecordKey
import io.airbyte.cdk.read.cdc.DebeziumRecordValue
import io.airbyte.cdk.read.cdc.DebeziumSchemaHistory
import io.airbyte.cdk.read.cdc.DebeziumWarmStartState
import io.airbyte.cdk.read.cdc.DeserializedRecord
import io.airbyte.cdk.read.cdc.ValidDebeziumWarmStartState
import io.airbyte.cdk.util.Jsons
import io.debezium.connector.sqlserver.SqlServerConnector
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.kafka.connect.source.SourceRecord

data class MsSqlServerCdcPosition(val lsn: String) : Comparable<MsSqlServerCdcPosition> {
    override fun compareTo(other: MsSqlServerCdcPosition): Int {
        return lsn.compareTo(other.lsn)
    }
}

@Singleton
class MsSqlServerDebeziumOperations(private val configuration: MsSqlServerSourceConfiguration) :
    CdcPartitionsCreatorDebeziumOperations<MsSqlServerCdcPosition>,
    CdcPartitionReaderDebeziumOperations<MsSqlServerCdcPosition> {

    private val log = KotlinLogging.logger {}

    override fun deserializeRecord(
        key: DebeziumRecordKey,
        value: DebeziumRecordValue,
        stream: Stream,
    ): DeserializedRecord {
        val before: JsonNode = value.before
        val after: JsonNode = value.after
        val isDelete: Boolean = after.isNull
        // Use either `before` or `after` as the record data, depending on the nature of the change.
        val data: ObjectNode = (if (isDelete) before else after) as ObjectNode

        // Add CDC metadata to the data object
        value.source["commit_lsn"]?.asText()?.let { data.put("_ab_cdc_lsn", it) }
        value.source["ts_ms"]?.asLong()?.let { data.put("_ab_cdc_updated_at", it) }
        if (isDelete) {
            value.source["ts_ms"]?.asLong()?.let { data.put("_ab_cdc_deleted_at", it) }
        }

        return DeserializedRecord(
            data = data,
            changes = emptyMap() // Field-level change tracking not currently supported by the CDK
        )
    }

    override fun position(recordValue: DebeziumRecordValue): MsSqlServerCdcPosition? {
        val commitLsn = recordValue.source["commit_lsn"]?.asText()
        return commitLsn?.let { MsSqlServerCdcPosition(it) }
    }

    override fun position(sourceRecord: SourceRecord): MsSqlServerCdcPosition? {
        val sourceValue = sourceRecord.value() as? Map<*, *> ?: return null
        val source = sourceValue["source"] as? Map<*, *> ?: return null
        val commitLsn = source["commit_lsn"] as? String ?: return null
        return MsSqlServerCdcPosition(commitLsn)
    }

    override fun position(offset: DebeziumOffset): MsSqlServerCdcPosition {
        if (offset.wrapped.size != 1) {
            throw IllegalArgumentException("Expected exactly 1 key in $offset")
        }
        val offsetValue = offset.wrapped.values.first() as ObjectNode
        val commitLsn = offsetValue["commit_lsn"].asText()
        return MsSqlServerCdcPosition(commitLsn)
    }

    override fun serializeState(
        offset: DebeziumOffset,
        schemaHistory: DebeziumSchemaHistory?
    ): JsonNode {
        val stateNode = Jsons.objectNode()

        // Serialize offset
        val offsetNode: JsonNode =
            Jsons.objectNode().apply {
                for ((k, v) in offset.wrapped) {
                    put(Jsons.writeValueAsString(k), Jsons.writeValueAsString(v))
                }
            }
        stateNode.set<JsonNode>("mssql_cdc_offset", offsetNode)

        // Serialize schema history if present
        if (schemaHistory != null) {
            val uncompressedString: String =
                schemaHistory.wrapped.joinToString(separator = "\n") {
                    io.debezium.document.DocumentWriter.defaultWriter().write(it.document())
                }

            if (uncompressedString.length <= 1024 * 1024) { // 1MB threshold
                stateNode.put("mssql_db_history", uncompressedString)
            } else {
                stateNode.put("is_compressed", true)
                val baos = java.io.ByteArrayOutputStream()
                java.util.zip.GZIPOutputStream(baos).writer(Charsets.UTF_8).use {
                    it.write(uncompressedString)
                }
                val encoded = java.util.Base64.getEncoder().encodeToString(baos.toByteArray())
                stateNode.put("mssql_db_history", "\"$encoded\"")
            }
        }

        return Jsons.objectNode().apply { set<JsonNode>("state", stateNode) }
    }

    override fun deserializeState(opaqueStateValue: JsonNode): DebeziumWarmStartState {
        return try {
            val debeziumState = deserializeStateUnvalidated(opaqueStateValue)
            ValidDebeziumWarmStartState(debeziumState.offset, debeziumState.schemaHistory)
        } catch (e: Exception) {
            log.error(e) { "Error deserializing incumbent state value." }
            AbortDebeziumWarmStartState("Error deserializing incumbent state value: ${e.message}")
        }
    }

    private fun deserializeStateUnvalidated(
        opaqueStateValue: JsonNode
    ): UnvalidatedDeserializedState {
        val stateNode: ObjectNode = opaqueStateValue["state"] as ObjectNode

        // Deserialize offset
        val offsetNode: ObjectNode = stateNode["mssql_cdc_offset"] as ObjectNode
        val offsetMap: Map<JsonNode, JsonNode> =
            offsetNode
                .fields()
                .asSequence()
                .map { (k, v) -> Jsons.readTree(k) to Jsons.readTree(v.textValue()) }
                .toMap()

        if (offsetMap.size != 1) {
            throw RuntimeException("Offset object should have 1 key in $opaqueStateValue")
        }
        val offset = DebeziumOffset(offsetMap)

        // Deserialize schema history if present
        val schemaNode: JsonNode =
            stateNode["mssql_db_history"] ?: return UnvalidatedDeserializedState(offset)
        val isCompressed: Boolean = stateNode["is_compressed"]?.asBoolean() ?: false

        val uncompressedString: String =
            if (isCompressed) {
                // Handle compressed schema history (similar to MySQL)
                val textValue: String = schemaNode.textValue()
                val compressedBytes: ByteArray =
                    textValue.substring(1, textValue.length - 1).toByteArray(Charsets.UTF_8)
                val decoded = java.util.Base64.getDecoder().decode(compressedBytes)
                java.util.zip
                    .GZIPInputStream(java.io.ByteArrayInputStream(decoded))
                    .reader(Charsets.UTF_8)
                    .readText()
            } else {
                schemaNode.textValue()
            }

        // Parse schema history records
        val schemaHistoryList: List<io.debezium.relational.history.HistoryRecord> =
            uncompressedString
                .lines()
                .filter { it.isNotBlank() }
                .map {
                    io.debezium.relational.history.HistoryRecord(
                        io.debezium.document.DocumentReader.defaultReader().read(it)
                    )
                }

        return UnvalidatedDeserializedState(offset, DebeziumSchemaHistory(schemaHistoryList))
    }

    data class UnvalidatedDeserializedState(
        val offset: DebeziumOffset,
        val schemaHistory: DebeziumSchemaHistory? = null,
    )

    /**
     * Gets the current maximum LSN from SQL Server for CDC cold start. This follows the pattern
     * from the old MSSQL connector.
     */
    private fun getCurrentMaxLsn(): String {
        // Create connection using the configuration's JDBC properties
        val url = configuration.jdbcUrlFmt.format(configuration.realHost, configuration.realPort)
        val properties =
            java.util.Properties().apply {
                configuration.jdbcProperties.forEach { (key, value) -> setProperty(key, value) }
            }

        java.sql.DriverManager.getConnection(url, properties).use { connection ->
            connection.createStatement().use { statement ->
                // Query sys.fn_cdc_get_max_lsn() - no need for USE statement since connection is
                // already to the right database
                val query = "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn;"
                statement.executeQuery(query).use { resultSet ->
                    return if (resultSet.next()) {
                        val lsnBytes = resultSet.getBytes("max_lsn")
                        if (lsnBytes != null) {
                            // Convert LSN bytes to hex string format that Debezium expects
                            lsnBytes.joinToString("") { "%02x".format(it) }
                        } else {
                            "00000000:00000000:0000" // Default starting LSN
                        }
                    } else {
                        "00000000:00000000:0000" // Default starting LSN
                    }
                }
            }
        }
    }

    override fun generateColdStartOffset(): DebeziumOffset {
        val currentLsn = getCurrentMaxLsn()
        val databaseName = configuration.databaseName

        // Create offset structure that matches SQL Server Debezium connector format
        val key =
            Jsons.arrayNode().apply {
                add(databaseName)
                add(
                    Jsons.objectNode().apply {
                        put("server", databaseName)
                        put("database", databaseName)
                    }
                )
            }
        val value =
            Jsons.objectNode().apply {
                put("commit_lsn", currentLsn)
                put("snapshot", true)
                put("snapshot_completed", true)
            }

        val offset = DebeziumOffset(mapOf(key to value))
        log.info { "Constructed SQL Server CDC cold start offset with LSN: $currentLsn" }
        return offset
    }

    override fun generateColdStartProperties(streams: List<Stream>): Map<String, String> {
        val databaseName = configuration.databaseName
        return mapOf(
            "name" to databaseName,
            "topic.prefix" to databaseName,
            "connector.class" to SqlServerConnector::class.java.name,
            "include.schema.changes" to "false",
            "provide.transaction.metadata" to "false",
            "binary.handling.mode" to "base64",
            "snapshot.mode" to "initial",
            "snapshot.locking.mode" to "none",
            "database.hostname" to configuration.realHost,
            "database.port" to configuration.realPort.toString(),
            "database.user" to configuration.jdbcProperties["user"].toString(),
            "database.password" to configuration.jdbcProperties["password"].toString(),
            "database.dbname" to databaseName,
            "database.names" to databaseName,
            "database.encrypt" to (configuration.jdbcProperties["encrypt"]!!),
            "database.trustServerCertificate" to
                (configuration.jdbcProperties["trustServerCertificate"]!!),
            // Offset storage configuration
            "offset.storage" to "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename" to "/tmp/debezium-mssql-offsets-${databaseName}.dat",
            "offset.flush.interval.ms" to "60000",
            // Register the MSSQL custom converter
            "converters" to "mssql",
            "mssql.type" to MsSqlServerDebeziumConverter::class.java.name
        )
    }

    override fun generateWarmStartProperties(streams: List<Stream>): Map<String, String> {
        return generateColdStartProperties(streams) + mapOf("snapshot.mode" to "never")
    }

    override fun findStreamName(key: DebeziumRecordKey, value: DebeziumRecordValue): String? {
        return value.source["table"]?.asText()
    }

    override fun findStreamNamespace(key: DebeziumRecordKey, value: DebeziumRecordValue): String? {
        return value.source["schema"]?.asText()
    }
}
