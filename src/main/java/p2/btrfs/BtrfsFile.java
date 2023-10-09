package p2.btrfs;

import p2.storage.EmptyStorageView;
import p2.storage.Interval;
import p2.storage.Storage;
import p2.storage.StorageView;

import java.util.Arrays;
import java.util.List;

/**
 * A file in a Btrfs file system. it uses a B-tree to store the intervals that hold the file's data.
 */
public class BtrfsFile {

    /**
     * The storage in which the file is stored.
     */
    private final Storage storage;

    /**
     * The name of the file.
     */
    private final String name;

    /**
     * The degree of the B-tree.
     */
    private final int degree;

    private final int maxKeys;

    /**
     * The root node of the B-tree.
     */
    private BtrfsNode root;

    /**
     * The total size of the file.
     */
    private int size;

    /**
     * Creates a new {@link BtrfsFile} instance.
     *
     * @param name    the name of the file.
     * @param storage the storage in which the file is stored.
     * @param degree  the degree of the B-tree.
     */
    public BtrfsFile(String name, Storage storage, int degree) {
        this.name = name;
        this.storage = storage;
        this.degree = degree;
        maxKeys = 2 * degree - 1;
        root = new BtrfsNode(degree);
    }

    /**
     * Reads all data from the file.
     *
     * @return a {@link StorageView} containing all data that is stored in this file.
     */
    public StorageView readAll() {
        return readAll(root);
    }

    /**
     * Reads all data from the given node.
     *
     * @param node the node to read from.
     * @return a {@link StorageView} containing all data that is stored in this file.
     */
    private StorageView readAll(BtrfsNode node) {

        StorageView view = new EmptyStorageView(storage);

        for (int i = 0; i < node.size; i++) {
            // before i-th key and i-th child.

            // read from i-th child if it exists
            if (node.children[i] != null) {
                view = view.plus(readAll(node.children[i]));
            }

            Interval key = node.keys[i];

            // read from i-th key
            view = view.plus(storage.createView(new Interval(key.start(), key.length())));
        }

        // read from last child if it exists
        if (node.children[node.size] != null) {
            view = view.plus(readAll(node.children[node.size]));
        }

        return view;
    }

    /**
     * Reads the given amount of data from the file starting at the given start position.
     *
     * @param start  the start position.
     * @param length the amount of data to read.
     * @return a {@link StorageView} containing the data that was read.
     */
    public StorageView read(int start, int length) {
        return read(start, length, root, 0, 0);
    }

    /**
     * Reads the given amount of data from the given node starting at the given start position.
     *
     * @param start            the start position.
     * @param length           the amount of data to read.
     * @param node             the current node to read from.
     * @param cumulativeLength the cumulative length of the intervals that have been visited so far.
     * @param lengthRead       the amount of data that has been read so far.
     * @return a {@link StorageView} containing the data that was read.
     */
    private StorageView read(int start, int length, BtrfsNode node, int cumulativeLength, int lengthRead) {
        // create the view to store the data in
        StorageView view = new EmptyStorageView(storage);
        // index in the current node
        int index = 0;
        // whether the child node at the current index has been added to the cumulative length
        boolean childAdded = false;
        // find index
        while (index < node.size) {
            if (cumulativeLength + node.childLengths[index] <= start + lengthRead) {
                cumulativeLength += node.childLengths[index];
                childAdded = true;
            } else break;
            if (cumulativeLength + node.keys[index].length() <= start + lengthRead) {
                cumulativeLength += node.keys[index].length();
                childAdded = false;
            } else break;
            index++;
        }
        // the position to start reading from is now in the next object, this can be a child or a key
        // while inside the node and there is still stuff to be read
        while (index < node.size && lengthRead < length) {
            // if the node is a leaf, skip the child reading step entirely
            if (childAdded || node.isLeaf()) {
                // if the child at the index has been added, the key now needs to be read
                Interval nextInterval = node.keys[index];
                // determine the position to start reading from in the next interval
                int startPositionInInterval = start + lengthRead - cumulativeLength;
                // determine the length to read in the next interval
                int readingLength = Math.min(nextInterval.length() - startPositionInInterval, length - lengthRead);
                // read the calculated length from the next interval
                view = view.plus(storage.createView(new Interval(nextInterval.start() + startPositionInInterval, readingLength)));
                // add the amount read to lengthRead
                lengthRead += readingLength;
                // don't forget to update the cumulative length
                cumulativeLength += nextInterval.length();
                // the second thing to read at a given index is the key, so only increase index here
                index++;
                // set childAdded
                childAdded = false;
            } else {
                // if the child at index has not been added, get the  storage view  via recursive call
                StorageView viewOfChild = read(start, length, node.children[index], cumulativeLength, lengthRead);
                view = view.plus(viewOfChild);
                // now set the length read and cumulative length variables
                lengthRead += viewOfChild.length();
                cumulativeLength += node.childLengths[index];
                // set childAdded
                childAdded = true;
            }
        }
        // there might be a need to read the last child, but only if the node is not a leaf
        if (lengthRead < length && !node.isLeaf()) {
            // read last child
            view = view.plus(read(start, length, node.children[index], cumulativeLength, lengthRead));
            // since integers are passed by value, there is no need to set the length read and the cumulative length anymore
        }
        // once everything has been added, return the view
        return view;
    }

    /**
     * Insert the given data into the file starting at the given start position.
     *
     * @param start     the start position.
     * @param intervals the intervals to write to.
     * @param data      the data to write into the storage.
     */
    public void insert(int start, List<Interval> intervals, byte[] data) {

        // fill the intervals with the data
        int dataPos = 0;
        for (Interval interval : intervals) {
            storage.write(interval.start(), data, dataPos, interval.length());
            dataPos += interval.length();
        }

        size += data.length;

        int insertionSize = data.length;

        // findInsertionPosition assumes that the current node is not full
        if (root.isFull()) {
            split(new IndexedNodeLinkedList(null, root, 0));
        }

        insert(intervals, findInsertionPosition(new IndexedNodeLinkedList(
            null, root, 0), start, 0, insertionSize, null), insertionSize);

    }

    /**
     * Inserts the given data into the given leaf at the given index.
     *
     * @param intervals       the intervals to insert.
     * @param indexedLeaf     The node and index to insert at.
     * @param remainingLength the remaining length of the data to insert.
     */
    private void insert(List<Interval> intervals, IndexedNodeLinkedList indexedLeaf, int remainingLength) {
        // node
        BtrfsNode node = indexedLeaf.node;
        // start by filling the current node as long as it is not full and there are intervals left
        while (node.size < maxKeys && !intervals.isEmpty()) {
            // shift everything right of the current index
            System.arraycopy(node.keys, indexedLeaf.index, node.keys, indexedLeaf.index + 1, node.size - indexedLeaf.index);
            // insert interval at index and remove it from the list
            node.keys[indexedLeaf.index] = intervals.remove(0);
            // remaining length is a bit special because it does not track the number of intervals, but their cumulative length
            remainingLength -= node.keys[indexedLeaf.index].length();
            // adjust values
            indexedLeaf.index++;
            node.size++;
        }
        // if there are no more intervals, exit
        if (intervals.isEmpty()) return;
        // if there are intervals left, start by splitting the current node
        split(indexedLeaf);
        // fix child lengths in parent
        // if the node has not changed, there is no need to do anything
        if (node != indexedLeaf.node) {
            // if the node has changed, the remaining length is still in the left node, so move it to the current one
            BtrfsNode parent = indexedLeaf.parent.node;
            parent.childLengths[indexedLeaf.parent.index - 1] -= remainingLength;
            parent.childLengths[indexedLeaf.parent.index] += remainingLength;
        }
        // once the lengths are fixed, continue with a recursive call
        insert(intervals, indexedLeaf, remainingLength);
    }

    /**
     * Finds the leaf node and index at which new intervals should be inserted given a start position.
     * It ensures that the start position is not in the middle of an existing interval
     * and updates the childLengths of the visited nodes.
     *
     * @param indexedNode      The current Position in the tree.
     * @param start            The start position of the intervals to insert.
     * @param cumulativeLength The length of the intervals in the tree up to the current node and index.
     * @param insertionSize    The total size of the intervals to insert.
     * @param splitKey         The right half of the interval that had to be split to ensure that the start position
     *                         is not in the middle of an interval. It will be inserted once the leaf node is reached.
     *                         If no split was necessary, this is null.
     * @return The leaf node and index, as well as the path to it, at which the intervals should be inserted.
     */
    private IndexedNodeLinkedList findInsertionPosition(IndexedNodeLinkedList indexedNode,
                                                        int start,
                                                        int cumulativeLength,
                                                        int insertionSize,
                                                        Interval splitKey) {
        // save original state
        int startCumLength = cumulativeLength;
        int startIndex = indexedNode.index;
        // extract node to operate on
        BtrfsNode node = indexedNode.node;
        // keep track of the index in the node
        int index = indexedNode.index;
        // track whether the next object in the node is a child node or an interval
        boolean nextIsChild = true;
        // find index
        while (index < node.size) {
            if (cumulativeLength + node.childLengths[index] < start) {
                cumulativeLength += node.childLengths[index];
                nextIsChild = false;
            } else break;
            // equal is accepted here because that way the position will be right behind the key at index which is the correct one
            if (cumulativeLength + node.keys[index].length() <= start) {
                cumulativeLength += node.keys[index].length();
                nextIsChild = true;
            } else break;
            index++;
        }
        // fix index in the indexed node as it has now changed
        indexedNode.index = index;
        // the loop has terminated, meaning that either index is equal to size, so the last child is where the position can be found
        // or the added value of the next object is bigger than start, therefore the index is in the next object
        // first, handle the case of the current node being a leaf
        if (node.isLeaf()) {
            // there are only keys now, the nextIsChild variable is useless here
            if (cumulativeLength == start) {
                // no splitting necessary
                if (splitKey != null) {
                    // shift to create space
                    System.arraycopy(node.keys, 0, node.keys, 1, node.size);
                    // if splitKey exists, it needs to be inserted at position 0
                    node.keys[0] = splitKey;
                    // increase size to account for the inserted key
                    node.size++;
                }
            } else {
                // splitting is needed
                // save original Interval
                Interval leftInterval = node.keys[index];
                // calculate lengths
                int leftIntervalLength = start - cumulativeLength;
                int rightIntervalLength = leftInterval.length() - leftIntervalLength;
                // create room for the right interval by shifting everything right of the left interval by one
                System.arraycopy(node.keys, index + 1, node.keys, index + 2, node.size - (index + 1));
                // set up new intervals with the adjusted values
                node.keys[index] = new Interval(leftInterval.start(), leftIntervalLength);
                node.keys[index + 1] = new Interval(leftInterval.start() + leftIntervalLength, rightIntervalLength);
                // return the list, index + 1 is the insertion position due to the split
                indexedNode.index++;
                // account for increased size
                node.size++;
            }
            // everything has been set, so return the current node as it is the one containing the insertion position
            return indexedNode;
        }
        // if it is a child, just continue there and split if necessary, also covers the case where the position is right behind a key
        if (nextIsChild) {
            // if the node at the index is full, it needs to be split
            if (node.children[index].isFull()) {
                // leftmost index for good measure, it should not matter
                split(new IndexedNodeLinkedList(indexedNode, node.children[index], 0));
                // restore state and start over
                indexedNode.index = startIndex;
                return findInsertionPosition(indexedNode, start, startCumLength, insertionSize, splitKey);
            }
            // adjust child length
            node.childLengths[index] += insertionSize;
            // now that the child is not full, the insertion position can be looked for in it
            return findInsertionPosition(new IndexedNodeLinkedList(indexedNode, node.children[index], 0), start, cumulativeLength, insertionSize, splitKey);
        } else {
            // if it is not a child, it's a key that might need to be split
            // if this is true, the position is right before the key
            if (cumulativeLength == start) {
                // ensure that the rightmost position is chosen not only during potential splitting
                if (node.children[index].isFull()) {
                    split(new IndexedNodeLinkedList(indexedNode, node.children[index], node.children[index].size - 1));
                    // restore state and start over
                    indexedNode.index = startIndex;
                    return findInsertionPosition(indexedNode, start, startCumLength, insertionSize, splitKey);
                }
                // adjust child length
                node.childLengths[index] += insertionSize;
                return findInsertionPosition(new IndexedNodeLinkedList(indexedNode, node.children[index], node.children[index].size), start, cumulativeLength, insertionSize, splitKey);
            } else {
                // the key at index needs to be split
                // save original Interval
                Interval leftInterval = node.keys[index];
                // calculate lengths
                int leftIntervalLength = start - cumulativeLength;
                int rightIntervalLength = leftInterval.length() - leftIntervalLength;
                // create right interval
                Interval rightInterval = new Interval(leftInterval.start() + leftIntervalLength, rightIntervalLength);
                // adjust left interval
                node.keys[index] = new Interval(leftInterval.start(), leftIntervalLength);
                // don't forget cumulativeLength
                cumulativeLength += node.keys[index].length();
                // assign splitKey
                splitKey = rightInterval;
                insertionSize += splitKey.length();
                // the index of the indexed node also needs to be increased
                indexedNode.index++;
                // split if necessary
                if (node.children[index + 1].isFull()) {
                    // leftmost index because that is the goal
                    split(new IndexedNodeLinkedList(indexedNode, node.children[index + 1], 0));
                    // restore state and start over
                    indexedNode.index = startIndex;
                    return findInsertionPosition(indexedNode, start, startCumLength, insertionSize, splitKey);
                }
                // adjust child length
                node.childLengths[index + 1] += insertionSize;
                // recursive call, now with splitKey, also increase index by one because of the splitting
                return findInsertionPosition(new IndexedNodeLinkedList(indexedNode, node.children[index + 1], 0), start, cumulativeLength, insertionSize, splitKey);
            }
        }
    }

    /**
     * Splits the given node referenced via the {@linkplain IndexedNodeLinkedList indexedNode} parameter in the middle. <br>
     * The method ensures that the given indexedNode points to correct {@linkplain IndexedNodeLinkedList#node node} and {@linkplain IndexedNodeLinkedList#index index} after the split.
     *
     * @param indexedNode The path to the node to split, represented by a {@link IndexedNodeLinkedList}
     * @see IndexedNodeLinkedList
     */
    private void split(IndexedNodeLinkedList indexedNode) {
        // create node variable to operate on
        BtrfsNode node = indexedNode.node;
        // new node for the right values
        BtrfsNode rightNode = new BtrfsNode(this.degree);
        // value will be assigned later based on whether the node being split is the root or not
        BtrfsNode parent;
        // the index of the left node (the one to split) in the parent node
        int index = 0;
        // whether the node to be split is the root
        boolean rootSplit = node == this.root;
        // set up the parent based on whether the root needs to be split or not
        if (rootSplit) {
            // create new root node
            parent = new BtrfsNode(this.degree);
            // account for new root in the IndexedNodeLinkedList
            indexedNode.parent = new IndexedNodeLinkedList(null, parent, 0);
            // fix the root reference
            this.root = parent;
            // set up the new root, its left child is the old root, and it contains 0 keys
            parent.size = 0;
            parent.children[index] = node;
        } else {
            // before the splitting happens, the parent needs to be looked at; if the parent is full, split parent as well
            if (indexedNode.parent.node.isFull()) {
                // split the parent, this might change things, so only assign the parent variable after the splitting
                split(indexedNode.parent);
            }
            // now the parent will be able to hold the node when it is split
            parent = indexedNode.parent.node;
            // extract index in parent node
            index = indexedNode.parent.index;
            // insert middle key of node in parent at the correct position; start by making room for the new key
            System.arraycopy(parent.keys, index, parent.keys, index + 1, parent.size - index);
            // move children and their lengths
            System.arraycopy(parent.children, index + 1, parent.children, index + 2, parent.size - index);
            System.arraycopy(parent.childLengths, index + 1, parent.childLengths, index + 2, parent.size - index);
        }
        // insert the key
        parent.keys[index] = node.keys[degree - 1];
        // insert the right node
        parent.children[index + 1] = rightNode;
        // new stuff has been inserted, increase size
        parent.size++;
        // set up right child by giving it half of the values, the array is full if a node is split
        System.arraycopy(node.keys, degree, rightNode.keys, 0, degree - 1);
        System.arraycopy(node.children, degree, rightNode.children, 0, degree);
        System.arraycopy(node.childLengths, degree, rightNode.childLengths, 0, degree);
        // first, truncate the array the desired length, then pad it to its original length
        node.keys = Arrays.copyOf(Arrays.copyOf(node.keys, degree - 1), 2 * degree - 1);
        node.children = Arrays.copyOf(Arrays.copyOf(node.children, degree), 2 * degree);
        node.childLengths = Arrays.copyOf(Arrays.copyOf(node.childLengths, degree), 2 * degree);
        // set the children's sizes
        node.size = degree - 1;
        rightNode.size = degree - 1;
        // calculate the length of the right node and set it accordingly in parent
        parent.childLengths[index + 1] = Arrays.stream(rightNode.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(rightNode.childLengths).sum();
        // the length calculation at index varies depending on whether the node has been split or not
        if (rootSplit)
            parent.childLengths[index] = this.size - (parent.childLengths[index + 1] + parent.keys[index].length());
        else parent.childLengths[index] -= parent.childLengths[index + 1] + parent.keys[index].length();
        // check whether the linked list needs to be adjusted
        if (indexedNode.index >= degree) {
            indexedNode.node = rightNode;
            indexedNode.index -= degree;
            indexedNode.parent.index++;
        }
    }

    /**
     * Writes the given data to the given intervals and stores them in the file starting at the given start position.
     * This method will override existing data starting at the given start position.
     *
     * @param start     the start position.
     * @param intervals the intervals to write to.
     * @param data      the data to write into the storage.
     */
    public void write(int start, List<Interval> intervals, byte[] data) {
        throw new UnsupportedOperationException("Not implemented yet"); //TODO H4 a): remove if implemented
    }

    /**
     * Removes the given number of bytes starting at the given position from this file.
     *
     * @param start  the start position of the bytes to remove
     * @param length the amount of bytes to remove
     */
    public void remove(int start, int length) {
        size -= length;
        int removed = remove(start, length, new IndexedNodeLinkedList(null, root, 0), 0, 0);

        // check if we have traversed the whole tree
        if (removed < length) {
            throw new IllegalArgumentException("start + length is out of bounds");
        } else if (removed > length) {
            throw new IllegalStateException("Removed more keys than wanted"); // sanity check
        }
    }

    /**
     * Removes the given number of bytes starting at the given position from the given node.
     *
     * @param start            the start position of the bytes to remove
     * @param length           the amount of bytes to remove
     * @param indexedNode      the current node to remove from
     * @param cumulativeLength the length of the intervals up to the current node and index
     * @param removedLength    the length of the intervals that have already been removed
     * @return the number of bytes that have been removed
     */
    private int remove(int start, int length, IndexedNodeLinkedList indexedNode, int cumulativeLength, int removedLength) {

        int initiallyRemoved = removedLength;
        boolean visitNextChild = true;

        // iterate over all children and keys
        for (; indexedNode.index < indexedNode.node.size; indexedNode.index++) {
            // before i-th child and i-th child.

            // check if we have removed enough
            if (removedLength > length) {
                throw new IllegalStateException("Removed more keys than wanted"); // sanity check
            } else if (removedLength == length) {
                return removedLength - initiallyRemoved;
            }

            // check if we have to visit the next child
            // we don't want to visit the child if we have already visited it but had to go back because the previous
            // key changed
            if (visitNextChild) {

                // remove from i-th child if start is in front of or in the i-th child, and it exists
                if (indexedNode.node.children[indexedNode.index] != null &&
                    start < cumulativeLength + indexedNode.node.childLengths[indexedNode.index]) {

                    // remove from child
                    final int removedInChild = remove(start, length,
                        new IndexedNodeLinkedList(indexedNode, indexedNode.node.children[indexedNode.index], 0),
                        cumulativeLength, removedLength);

                    // update removedLength
                    removedLength += removedInChild;

                    // update childLength of parent accordingly
                    indexedNode.node.childLengths[indexedNode.index] -= removedInChild;

                    // check if we have removed enough
                    if (removedLength == length) {
                        return removedLength - initiallyRemoved;
                    } else if (removedLength > length) {
                        throw new IllegalStateException("Removed more keys than wanted"); // sanity check
                    }
                }

                cumulativeLength += indexedNode.node.childLengths[indexedNode.index];
            } else {
                visitNextChild = true;
            }

            // get the i-th key
            Interval key = indexedNode.node.keys[indexedNode.index];

            // the key might not exist anymore
            if (key == null) {
                return removedLength - initiallyRemoved;
            }

            // if start is in the i-th key we just have to shorten the interval
            if (start > cumulativeLength && start < cumulativeLength + key.length()) {

                // calculate the new length of the key
                final int newLength = start - cumulativeLength;

                // update cumulativeLength before updating the key
                cumulativeLength += key.length();

                // update the key
                indexedNode.node.keys[indexedNode.index] = new Interval(key.start(), newLength);

                // update removedLength
                removedLength += key.length() - newLength;

                // continue with next key
                continue;
            }

            // if start is in front of or at the start of the i-th key we have to remove the key
            if (start <= cumulativeLength) {

                // if the key is longer than the length to be removed we just have to shorten the key
                if (key.length() > length - removedLength) {

                    final int newLength = key.length() - (length - removedLength);
                    final int newStart = key.start() + (key.length() - newLength);

                    // update the key
                    indexedNode.node.keys[indexedNode.index] = new Interval(newStart, newLength);

                    // update removedLength
                    removedLength += key.length() - newLength;

                    // we are done
                    return removedLength - initiallyRemoved;
                }

                // if we are in a leaf node we can just remove the key
                if (indexedNode.node.isLeaf()) {

                    ensureSize(indexedNode);

                    // move all keys after the removed key to the left
                    System.arraycopy(indexedNode.node.keys, indexedNode.index + 1,
                        indexedNode.node.keys, indexedNode.index, indexedNode.node.size - indexedNode.index - 1);

                    // remove (duplicated) last key
                    indexedNode.node.keys[indexedNode.node.size - 1] = null;

                    // update size
                    indexedNode.node.size--;

                    // update removedLength
                    removedLength += key.length();

                    // the next key moved one index to the left
                    indexedNode.index--;

                } else { // remove key from inner node

                    // try to replace with rightmost key of left child
                    if (indexedNode.node.children[indexedNode.index].size >= degree) {
                        final Interval removedKey = removeRightMostKey(new IndexedNodeLinkedList(indexedNode,
                            indexedNode.node.children[indexedNode.index], 0));

                        // update childLength of current node
                        indexedNode.node.childLengths[indexedNode.index] -= removedKey.length();

                        // update key
                        indexedNode.node.keys[indexedNode.index] = removedKey;

                        // update removedLength
                        removedLength += key.length();

                        // try to replace with leftmost key of right child
                    } else if (indexedNode.node.children[indexedNode.index + 1].size >= degree) {
                        final Interval removedKey = removeLeftMostKey(new IndexedNodeLinkedList(indexedNode,
                            indexedNode.node.children[indexedNode.index + 1], 0));

                        // update childLength of current node
                        indexedNode.node.childLengths[indexedNode.index + 1] -= removedKey.length();

                        // update key
                        indexedNode.node.keys[indexedNode.index] = removedKey;

                        // update removedLength
                        removedLength += key.length();

                        cumulativeLength += removedKey.length();

                        // we might have to remove the new key as well -> go back
                        indexedNode.index--;
                        visitNextChild = false; // we don't want to remove from the previous child again

                        continue;

                        // if both children have only degree - 1 keys we have to merge them and remove the key from the merged node
                    } else {

                        // save the length of the right child before merging because we have to add it to the
                        // cumulative length later
                        final int rightNodeLength = indexedNode.node.childLengths[indexedNode.index + 1];

                        ensureSize(indexedNode);

                        // merge the two children
                        mergeWithRightSibling(new IndexedNodeLinkedList(indexedNode,
                            indexedNode.node.children[indexedNode.index], 0));

                        // remove the key from the merged node
                        int removedInChild = remove(start, length, new IndexedNodeLinkedList(indexedNode,
                                indexedNode.node.children[indexedNode.index], degree - 1),
                            cumulativeLength, removedLength);

                        // update childLength of current node
                        indexedNode.node.childLengths[indexedNode.index] -= removedInChild;

                        // update removedLength
                        removedLength += removedInChild;

                        // add the right child to the cumulative length
                        cumulativeLength += rightNodeLength;

                        // merging with right child shifted the keys to the left -> we have to visit the previous key again
                        indexedNode.index--;
                        visitNextChild = false; // we don't want to remove from the previous child again
                    }

                }

            }

            // update cumulativeLength after visiting the i-th key
            cumulativeLength += key.length();

        } // only the last child is left

        // check if we have removed enough
        if (removedLength > length) {
            throw new IllegalStateException("Removed more keys than wanted"); // sanity check
        } else if (removedLength == length) {
            return removedLength - initiallyRemoved;
        }

        // remove from the last child if start is in front of or in the i-th child, and it exists
        if (indexedNode.node.children[indexedNode.node.size] != null &&
            start <= cumulativeLength + indexedNode.node.childLengths[indexedNode.node.size]) {

            // remove from child
            int removedInChild = remove(start, length, new IndexedNodeLinkedList(indexedNode,
                indexedNode.node.children[indexedNode.node.size], 0), cumulativeLength, removedLength);

            // update childLength of parent accordingly
            indexedNode.node.childLengths[indexedNode.node.size] -= removedInChild;

            // update removedLength
            removedLength += removedInChild;
        }

        return removedLength - initiallyRemoved;
    }

    /**
     * Removes the rightmost key of the given node if it is a leaf.
     * Otherwise, it will remove the rightmost key of the last child.
     *
     * @param indexedNode the node to remove the rightmost key from.
     * @return the removed key.
     */
    private Interval removeRightMostKey(IndexedNodeLinkedList indexedNode) {
        // if the node is a leaf, no more recursion is needed
        if (indexedNode.node.isLeaf()) {
            // keep the leaf legal
            ensureSize(indexedNode);
            // the node
            BtrfsNode node = indexedNode.node;
            // save the rightmost interval
            Interval theInterval = node.keys[node.size - 1];
            // remove the interval from the node
            node.keys[node.size - 1] = null;
            node.size--;
            // simply return the interval
            return theInterval;
        } else {
            // the node
            BtrfsNode node = indexedNode.node;
            // set index in future parent
            indexedNode.index = node.size;
            // if the node is not a leaf, a recursive call is needed to obtain the interval, choose the rightmost index
            Interval theInterval = removeRightMostKey(new IndexedNodeLinkedList(indexedNode, node.children[node.size], node.children[node.size].size));
            // adjust child length
            node.childLengths[node.size] = Arrays.stream(node.children[node.size].keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(node.children[node.size].childLengths).sum();
            // then simply return the node
            return theInterval;
        }
    }

    /**
     * Removes the leftmost key of the given node if it is a leaf.
     * Otherwise, it will remove the leftmost key of the first child.
     *
     * @param indexedNode the node to remove the leftmost key from.
     * @return the removed key.
     */
    private Interval removeLeftMostKey(IndexedNodeLinkedList indexedNode) {
        // if the node is a leaf, no more recursion is needed
        if (indexedNode.node.isLeaf()) {
            // keep the leaf legal
            ensureSize(indexedNode);
            // the node
            BtrfsNode node = indexedNode.node;
            // save the leftmost interval
            Interval theInterval = node.keys[0];
            // shift keys, removing the interval from the node in the process
            System.arraycopy(node.keys, 1, node.keys, 0, node.size - 1);
            // decrease size
            node.size--;
            // simply return the interval
            return theInterval;
        } else {
            // the node
            BtrfsNode node = indexedNode.node;
            // set index in future parent
            indexedNode.index = 0;
            // if the node is not a leaf, a recursive call is needed to obtain the interval, choose the leftmost index
            Interval theInterval = removeLeftMostKey(new IndexedNodeLinkedList(indexedNode, node.children[0], 0));
            // adjust child length
            // the stream method does not work because the last key is not set to null
            // node.childLengths[0] = Arrays.stream(node.children[0].keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(node.children[0].childLengths).sum();
            node.childLengths[0] -= theInterval.length();
            // then simply return the node
            return theInterval;
        }
    }

    /**
     * Ensures that the given node has at least degree keys if it is not the root.
     * If the node has less than degree keys, it will try to rotate a key from a sibling or merge with a sibling.
     *
     * @param indexedNode the node to ensure the size of.
     */
    private void ensureSize(IndexedNodeLinkedList indexedNode) {
        // the node
        BtrfsNode node = indexedNode.node;
        // do nothing if any of these criteria is met
        if (node == this.root || node.size >= degree) return;
        // first, check for rotate methods
        // start by keeping track of the parent node
        BtrfsNode parent = indexedNode.parent.node;
        // node index in parent
        int indexInParent = indexedNode.parent.index;
        // check for right rotation
        if (indexInParent + 1 < parent.children.length && parent.children[indexInParent + 1] != null && parent.children[indexInParent + 1].size >= degree)
            rotateFromRightSibling(indexedNode);
            // else, try left rotation
        else if (indexInParent - 1 >= 0 && parent.children[indexInParent - 1] != null && parent.children[indexInParent - 1].size >= degree)
            rotateFromLeftSibling(indexedNode);
            // if one of the rotations was possible, do a merge instead
        else { // otherwise merge with one of the siblings since they are both at the lowest possible size
            // first, ensure that the parent is able to support the merge
            ensureSize(indexedNode.parent);
            // since things might have changed after the recursive call, reset local variables
            parent = indexedNode.parent.node;
            indexInParent = indexedNode.parent.index;
            // attempt right merge if possible
            if (indexInParent + 1 < parent.children.length && parent.children[indexInParent + 1] != null)
                mergeWithRightSibling(indexedNode);
                // else do a left merge because this is now guaranteed to be possible
            else mergeWithLeftSibling(indexedNode);
            // deal with root being empty
            if (root.size == 0) {
                root = indexedNode.node;
            }
        }
    }

    /**
     * Merges the given node with its left sibling.
     * The method ensures that the given indexedNode points to correct node and index after the split.
     *
     * @param indexedNode the node to merge with its left sibling.
     */
    private void mergeWithLeftSibling(IndexedNodeLinkedList indexedNode) {
        // merge target
        BtrfsNode target = indexedNode.node;
        // parent
        BtrfsNode parent = indexedNode.parent.node;
        // target index in parent
        int indexInParent = indexedNode.parent.index;
        // left sibling
        BtrfsNode leftSibling = parent.children[indexInParent - 1];
        // shift everything in target in order to create space for the left sibling's entries and the parent key
        System.arraycopy(target.keys, 0, target.keys, leftSibling.size + 1, target.size);
        System.arraycopy(target.children, 0, target.children, leftSibling.size + 1, target.size + 1);
        System.arraycopy(target.childLengths, 0, target.childLengths, leftSibling.size + 1, target.size + 1);
        // copy the key from parent
        target.keys[leftSibling.size] = parent.keys[indexInParent - 1];
        // copy everything from the left sibling
        System.arraycopy(leftSibling.keys, 0, target.keys, 0, leftSibling.size);
        System.arraycopy(leftSibling.children, 0, target.children, 0, leftSibling.size + 1);
        System.arraycopy(leftSibling.childLengths, 0, target.childLengths, 0, leftSibling.size + 1);
        // adjust child length of target in parent
        parent.childLengths[indexInParent] += parent.childLengths[indexInParent - 1]+target.keys[leftSibling.size].length();
        // shift everything in parent to account for the left sibling
        System.arraycopy(parent.keys, indexInParent, parent.keys, indexInParent - 1, parent.size - indexInParent);
        System.arraycopy(parent.children, indexInParent, parent.children, indexInParent - 1, parent.size - indexInParent + 1);
        System.arraycopy(parent.childLengths, indexInParent, parent.childLengths, indexInParent - 1, parent.size - indexInParent + 1);
        // adjust sizes
        parent.size--;
        target.size += leftSibling.size + 1;
        // adjust indices
        indexedNode.parent.index--;
        indexedNode.index += leftSibling.size + 1;
        // null everything in parent that is no longer needed
        parent.keys = Arrays.copyOf(Arrays.copyOf(parent.keys, parent.size), 2 * degree - 1);
        parent.children = Arrays.copyOf(Arrays.copyOf(parent.children, parent.size + 1), 2 * degree);
        parent.childLengths = Arrays.copyOf(Arrays.copyOf(parent.childLengths, parent.size + 1), 2 * degree);
    }

    /**
     * Merges the given node with its right sibling.
     * The method ensures that the given indexedNode points to correct node and index after the split.
     *
     * @param indexedNode the node to merge with its right sibling.
     */
    private void mergeWithRightSibling(IndexedNodeLinkedList indexedNode) {
        // merge target
        BtrfsNode target = indexedNode.node;
        // parent
        BtrfsNode parent = indexedNode.parent.node;
        // target index in parent
        int indexInParent = indexedNode.parent.index;
        // right sibling
        BtrfsNode rightSibling = parent.children[indexInParent + 1];
        // copy parent key to target
        target.keys[target.size] = parent.keys[indexInParent];
        // adjust size
        target.size++;
        // copy right sibling's keys to target
        System.arraycopy(rightSibling.keys, 0, target.keys, target.size, rightSibling.size);
        // copy right sibling's children and their respective lengths to target
        System.arraycopy(rightSibling.children, 0, target.children, target.size, rightSibling.size + 1);
        System.arraycopy(rightSibling.childLengths, 0, target.childLengths, target.size, rightSibling.size + 1);
        // fix the child length for target
        parent.childLengths[indexInParent] += parent.childLengths[indexInParent + 1]+target.keys[rightSibling.size].length();
        // move keys right of the index in parent to account for the removed key
        System.arraycopy(parent.keys, indexInParent + 1, parent.keys, indexInParent, parent.size - (indexInParent + 1));
        // also adjust children and child lengths
        System.arraycopy(parent.children, indexInParent + 2, parent.children, indexInParent + 1, parent.size - (indexInParent + 1));
        System.arraycopy(parent.childLengths, indexInParent + 2, parent.childLengths, indexInParent + 1, parent.size - (indexInParent + 1));
        // adjust sizes
        parent.size--;
        target.size += rightSibling.size;
        // null everything in parent that is no longer needed
        parent.keys = Arrays.copyOf(Arrays.copyOf(parent.keys, parent.size), 2 * degree - 1);
        parent.children = Arrays.copyOf(Arrays.copyOf(parent.children, parent.size + 1), 2 * degree);
        parent.childLengths = Arrays.copyOf(Arrays.copyOf(parent.childLengths, parent.size + 1), 2 * degree);
    }

    /**
     * Rotates an interval from the left sibling via the parent to the given node.
     *
     * @param indexedNode the node to rotate to.
     */
    private void rotateFromLeftSibling(IndexedNodeLinkedList indexedNode) {
        // rotation target
        BtrfsNode target = indexedNode.node;
        // parent
        BtrfsNode parent = indexedNode.parent.node;
        // target index in parent
        int indexInParent = indexedNode.parent.index;
        // left sibling
        BtrfsNode leftSibling = parent.children[indexInParent - 1];
        // save the rightmost element from the right sibling
        Interval leftKey = leftSibling.keys[leftSibling.size - 1];
        // also save child and child length
        BtrfsNode leftChild = leftSibling.children[leftSibling.size];
        int leftChildLength = leftSibling.childLengths[leftSibling.size];
        // shift target's arrays to create room for the new values
        System.arraycopy(target.keys, 0, target.keys, 1, target.size);
        System.arraycopy(target.children, 0, target.children, 1, target.size + 1);
        System.arraycopy(target.childLengths, 0, target.childLengths, 1, target.size + 1);
        // add key in parent node to target
        target.keys[0] = parent.keys[indexInParent - 1];
        // add child and child length
        target.children[0] = leftChild;
        target.childLengths[0] = leftChildLength;
        // replace key in parent node with key from right sibling
        parent.keys[indexInParent - 1] = leftKey;
        // set last elements to null/0 in left sibling's arrays
        leftSibling.keys[leftSibling.size - 1] = null;
        leftSibling.children[leftSibling.size] = null;
        leftSibling.childLengths[leftSibling.size] = 0;
        // fix sizes
        target.size++;
        leftSibling.size--;
        // adjust child lengths
        parent.childLengths[indexInParent] += target.keys[0].length() + leftChildLength;
        parent.childLengths[indexInParent - 1] -= parent.keys[indexInParent - 1].length() + leftChildLength;
        // fix index in target node to account for the shift that happened
        indexedNode.index++;
    }

    /**
     * Rotates an interval from the right sibling via the parent to the given node.
     *
     * @param indexedNode the node to rotate to.
     */
    private void rotateFromRightSibling(IndexedNodeLinkedList indexedNode) {
        // rotation target
        BtrfsNode target = indexedNode.node;
        // parent
        BtrfsNode parent = indexedNode.parent.node;
        // target index in parent
        int indexInParent = indexedNode.parent.index;
        // right sibling
        BtrfsNode rightSibling = parent.children[indexInParent + 1];
        // save the leftmost element from the right sibling
        Interval rightKey = rightSibling.keys[0];
        // also save child and child length
        BtrfsNode rightChild = rightSibling.children[0];
        int rightChildLength = rightSibling.childLengths[0];
        // shift right sibling's arrays to fill the gap
        System.arraycopy(rightSibling.keys, 1, rightSibling.keys, 0, rightSibling.size - 1);
        System.arraycopy(rightSibling.children, 1, rightSibling.children, 0, rightSibling.size);
        System.arraycopy(rightSibling.childLengths, 1, rightSibling.childLengths, 0, rightSibling.size);
        // add key in parent node to target
        target.keys[target.size] = parent.keys[indexInParent];
        // add child and child length
        target.children[target.size + 1] = rightChild;
        target.childLengths[target.size + 1] = rightChildLength;
        // replace key in parent node with key from right sibling
        parent.keys[indexInParent] = rightKey;
        // set last elements to null/0 in right sibling's arrays
        rightSibling.keys[rightSibling.size - 1] = null;
        rightSibling.children[rightSibling.size] = null;
        rightSibling.childLengths[rightSibling.size] = 0;
        // fix sizes
        target.size++;
        rightSibling.size--;
        // adjust child lengths
        parent.childLengths[indexInParent] += target.keys[target.size - 1].length() + rightChildLength;
        parent.childLengths[indexInParent + 1] -= parent.keys[indexInParent].length() + rightChildLength;
    }

    /**
     * Checks if there are any adjacent intervals that are also point to adjacent bytes in the storage.
     * If there are such intervals, they are merged into a single interval.
     */
    public void shrink() {

        throw new UnsupportedOperationException("Not implemented yet"); //TODO H4 b): remove if implemented
    }

    /**
     * Returns the size of the file.
     * This is the sum of the length of all intervals or the amount of bytes used in the storage.
     *
     * @return the size of the file.
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns the name of the file.
     *
     * @return the name of the file.
     */
    public String getName() {
        return name;
    }
}
