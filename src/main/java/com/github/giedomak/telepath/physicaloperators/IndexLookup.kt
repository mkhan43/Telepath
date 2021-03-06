package com.github.giedomak.telepath.physicaloperators

import com.github.giedomak.telepath.datamodels.graph.PathPrefix
import com.github.giedomak.telepath.datamodels.graph.PathStream
import com.github.giedomak.telepath.datamodels.plans.PhysicalPlan

/**
 * Physical operator to lookup paths in the kPathIndex.
 *
 * Let's say we've got this physical plan:
 *
 *      INDEX_LOOKUP
 *        /  |  \
 *       a   b   c
 *
 * Then `a - b - c` forms the labeled path for which we are searching for paths in de index.
 *
 * @property physicalPlan The physical plan which holds the leafs which make up the pathId.
 */
class IndexLookup(override val physicalPlan: PhysicalPlan) : PhysicalOperator {

    /**
     * Evaluate the index lookup and stream the results.
     *
     * @return PathStream with the results of the index lookup.
     */
    override fun evaluate(): PathStream {
        return PathStream(
                physicalPlan.query.telepath,
                physicalPlan.query.telepath.kPathIndex.search(
                        PathPrefix(
                                physicalPlan.pathIdOfChildren()
                        )
                ),
                false
        )
    }

    /**
     * Cost of an index lookup is very cheap.
     */
    override fun cost(): Long {
        // Since an index lookup is always a leaf, we don't have cost of intermediate steps here.
        return 1
    }
}