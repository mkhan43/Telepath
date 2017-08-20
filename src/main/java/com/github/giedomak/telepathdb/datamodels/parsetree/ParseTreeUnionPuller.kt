/**
 * Copyright (C) 2016-2017 - All rights reserved.
 * This file is part of the telepathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.github.giedomak.telepathdb.datamodels.parsetree

import com.github.giedomak.telepathdb.utilities.Logger
import java.util.stream.Collectors

/**
 * Split a [ParseTree] on Union into multiple ParseTrees.
 *
 * Illustrative example: `"a/(b|c)/d"` becomes `["a/b/d", "a/c/d"]`
 */
object ParseTreeUnionPuller {

    /**
     * Removes the UNION operator from a ParseTree.
     *
     * Simply duplicate the tree and on the position of the UNION operator, the first tree
     * gets the left-child and the other tree gets the right-child.
     *
     * @param parseTree The ParseTree to parse.
     * @return List of ParseTrees each not containing the UNION operator.
     */
    fun parse(parseTree: ParseTree): List<ParseTree> {

        val parseTrees = mutableListOf<ParseTree>()
        parseTrees.add(parseTree)

        // Print the parsed ParseTree
        parseTree.print()

        // List to hold the ParseTrees which UNIONS in them
        var unionTrees = emptyList<ParseTree>()

        // Collect the ParseTrees which contain the UNION operator
        while ({
            unionTrees = parseTrees.stream()
                    .filter { it.contains(ParseTree.UNION) }
                    .collect(Collectors.toList())
            unionTrees
        }().isNotEmpty()) {

            // For each of those trees with the UNION operator in them
            for (tree in unionTrees) {

                // Split them immediately when the Root is the UNION operator
                if (tree.isRoot && tree.operator == ParseTree.UNION) {

                    // Remove the current tree from the list, and add its children to our parseTrees var
                    parseTrees.remove(tree)
                    for (child in tree.children) {
                        val cloned = child.clone() as ParseTree
                        cloned.isRoot = true // This cloned tree is now a root
                        parseTrees.add(cloned)
                    }
                    continue // Continue to the next parsetree containing UNION
                }

                // Deep clone the current tree
                val clone = tree.clone() as ParseTree

                // Recursively remove the first UNION we find doing a pre-order treewalk.
                // Replace the UNION node with its left child in the original tree, and with the
                // right child in the clone of the original tree.
                removeFirstUnion(tree, 0)
                removeFirstUnion(clone, 1)

                Logger.debug("UNIONNNN")
                tree.print()
                clone.print()

                // We still have to add the clone to the list
                parseTrees.add(clone)
            }
        }

        return parseTrees
    }

    /**
     * Recursively replace UNION nodes of parsetrees with its child, chosen by the param childChooser.
     *
     * We use a pre-order tree walk and return after we've replaced the first UNION with its child.
     *
     * @param tree              The tree we have to traverse finding the first occurrence of a UNION operator.
     * @param childChooserIndex Define if we have to replace the UNION node with its right or left child.
     * @return Boolean indicating if we've replaced a UNION node.
     */
    private fun removeFirstUnion(tree: ParseTree, childChooserIndex: Int): Boolean {

        // Return if we've reached a leaf
        if (tree.isLeaf) {
            return false
        }

        // Check each child for a UNION node.
        for (child in tree.children) {
            // Check if our child is a UNION node. If so, replace it with the childChooserIndex of our child.
            if (child.operator == ParseTree.UNION) {
                val index = tree.children.indexOf(child)
                tree.setChild(index, child.getChild(childChooserIndex)!! as ParseTree)
                // Return if we've found one, breaking the recursive call
                return true
            }
        }

        // Traverse to the left child if we haven't found a UNION node already
        for (child in tree.children) {
            if (removeFirstUnion(child as ParseTree, childChooserIndex)) {
                return true
            }
        }

        return false
    }
}
