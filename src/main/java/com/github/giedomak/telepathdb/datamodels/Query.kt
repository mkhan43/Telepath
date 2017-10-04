package com.github.giedomak.telepathdb.datamodels

import com.github.giedomak.telepathdb.TelepathDB
import com.github.giedomak.telepathdb.datamodels.graph.PathStream
import com.github.giedomak.telepathdb.datamodels.plans.LogicalPlan
import com.github.giedomak.telepathdb.datamodels.plans.PhysicalPlan
import com.github.giedomak.telepathdb.utilities.Logger

/**
 * Query model which is responsible for the-life-of-a-query.
 *
 * @property telepathDB Reference to the TelepathDB module.
 * @property input The input as given by the user.
 */
data class Query(val telepathDB: TelepathDB, val input: String) {

    private var logicalPlan: LogicalPlan? = null
    private var flattenedLogicalPlan: LogicalPlan? = null
    private var physicalPlan: PhysicalPlan? = null
    private var results: PathStream? = null

    private var startTime = System.currentTimeMillis()
    private var endTime = System.currentTimeMillis()

    fun parseInput() {
        val start = System.currentTimeMillis()
        logicalPlan = telepathDB.staticParser.parse(this)
        val ms = System.currentTimeMillis() - start

        Logger.debug("Logical plan: ($ms ms)")
        logicalPlan!!.print()
    }

    fun flattenLogicalPlan() {
        val start = System.currentTimeMillis()
        flattenedLogicalPlan = logicalPlan!!.clone().flatten()
        val ms = System.currentTimeMillis() - start

        Logger.debug("Flattened logical plan: ($ms ms)")
        flattenedLogicalPlan!!.print()
    }

    fun generatePhysicalPlan(): Long {
        val start = System.currentTimeMillis()
        physicalPlan = telepathDB.planner.generate(flattenedLogicalPlan!!)
        val ms = System.currentTimeMillis() - start

        Logger.debug("Physical plan: ($ms ms)")
        physicalPlan!!.print()

        return ms
    }

    fun evaluate(): Long {
        val start = System.currentTimeMillis()
        results = telepathDB.evaluationEngine.evaluate(physicalPlan!!)
        val ms = System.currentTimeMillis() - start

        Logger.debug("Evaluation step done in ($ms ms)")

        endTime = System.currentTimeMillis()

        return ms
    }

    fun printCount(force: Boolean = false) {
        // If the results were materialized, we actually get a supplier. Meaning we can access the stream again.
        if (results!!.materialize || force) {
            Logger.debug(">>>>> Actual number of results: " + results!!.paths.count())
        }
    }

    fun printResults(maxSize: Long = 10) {

        Logger.debug(">>>>> Estimated number of results: " + physicalPlan!!.cardinality())

        Logger.info(">>>>> Results limited to $maxSize:")
        results!!.paths.limit(maxSize).forEach { Logger.info(it) }

        Logger.info("Query evaluation in " + (endTime - startTime) + " ms")
        Logger.info("----------------------------")
    }
}