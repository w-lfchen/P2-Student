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
    private StorageView read(int start, int length, BtrfsNode node, int cumulativeLength, int lengthRead) { // TODO: test, check for off-by-one errors!!
        // create the view to store the data in
        StorageView view = new EmptyStorageView(storage);
        // find starting position
        // if the method is called, the position must be in the given node
        int index = 0; // index in the arrays
        int logicalAddress; // logical address to be worked with
        boolean childAdded = false; // whether the child at index has been added to cumulativeLength
        // determine index
        while (index < node.size) {
            if (cumulativeLength + node.childLengths[index] < start + lengthRead) {
                cumulativeLength += node.childLengths[index];
                childAdded = true;
            } else break;
            if (cumulativeLength + node.keys[index].length() < start + lengthRead) {
                cumulativeLength += node.keys[index].length();
            } else break;
            index++;
            childAdded = false;
        }
        // now the index has been determined, it may also be size-1, where a further check for the last child needs to be implemented
        logicalAddress = cumulativeLength; // first, add everything that has been skipped so far to the logical address
        if (childAdded) {
            // if the child at index has been added, the key needs to be read first, then continue from index+1
            // determine difference in logical address and start
            int diff = start + lengthRead - logicalAddress;
            // extract key for easy handling
            Interval key = node.keys[index];
            // determine how much of the key should be read
            int lengthToBeRead = Math.min(key.length() - diff, length);
            // read the key and append to view
            view = view.plus(storage.createView(new Interval(key.start() + diff, lengthToBeRead)));
            // set variables for further stuff
            logicalAddress = start + lengthToBeRead;
            lengthRead += lengthToBeRead;
            cumulativeLength += key.length();
            index++;
        } // index is now at a position where the child and then the key should be read, if some conditions are fulfilled
        while (lengthRead < length && index < node.size) {
            // read child at index
            view = view.plus(read(start, length, node.children[index], cumulativeLength, lengthRead));
            // adjust variables to keep track of what has been read
            cumulativeLength += node.childLengths[index];
            lengthRead += node.childLengths[index];
            // read key if necessary
            if (lengthRead < length) {
                // determine difference in logical address and start
                int diff = start + lengthRead - logicalAddress;
                // extract key for easy handling
                Interval key = node.keys[index];
                // determine how much of the key should be read
                int lengthToBeRead = Math.min(key.length() - diff, length);
                // read the key and append to view
                view = view.plus(storage.createView(new Interval(key.start() + diff, lengthToBeRead)));
                // set variables for further stuff
                logicalAddress = start + lengthToBeRead;
                lengthRead += lengthToBeRead;
                index++;
            }
        }
        // read last child if needed
        if (lengthRead < length) {
            view = view.plus(read(start, length, node.children[index], cumulativeLength, lengthRead));
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

        throw new UnsupportedOperationException("Not implemented yet"); //TODO H2 c): remove if implemented
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
    private IndexedNodeLinkedList findInsertionPosition(IndexedNodeLinkedList indexedNode, // TODO: test
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
        boolean nextIsChild = true; // whether the next object in the node is a child node or an interval
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
        // fix index as it has now changed
        indexedNode.index = index;
        // the loop has terminated, meaning that either index is equal to size, so the last child is where the position can be found
        // or the added value of the next object is bigger than start, therefore the index is in the next object
        // first, handle the case of the current node being a leaf
        if (node.isLeaf()){
            // there are only keys now, the nextIsChild variable is useless here
            if (cumulativeLength == start){
                // no splitting necessary
                if (splitKey != null){
                    // shift to create space
                    System.arraycopy(node.keys, 0, node.keys, 1, node.size);
                    // if splitKey exists, it needs to be inserted at position 0
                    node.keys[0] = splitKey;
                    // increase size to account for the inserted key
                    node.size++;
                }
                return indexedNode;
            } else {
                // splitting is needed
                // save original Interval
                Interval leftInterval = node.keys[index];
                // calculate lengths
                int leftIntervalLength = start-cumulativeLength;
                int rightIntervalLength = leftInterval.length() - leftIntervalLength;
                // create room for the right interval by shifting everything right of the left interval by one
                System.arraycopy(node.keys, index + 1, node.keys, index + 2, node.size-(index + 1));
                // set up new intervals with the adjusted values
                node.keys[index] = new Interval(leftInterval.start(), leftIntervalLength);
                node.keys[index + 1] = new Interval(leftInterval.start() + leftIntervalLength, rightIntervalLength);
                // return list, index + 1 is the insertion position
                indexedNode.index++;
                // account for increased size
                node.size++;
                return indexedNode;
            }
        }
        // if it is a child, just continue there and split if necessary, also covers the case where the position is right behind a key
        if (nextIsChild) {
            // TODO: something is wrong around here
            // it has to do with the splitting, maybe need to adjust some values afterwards
            // split is being called, then the wrong node is entered
            if (node.children[index].isFull()){
                // leftmost index for good measure
                split(new IndexedNodeLinkedList(indexedNode, node.children[index], 0));
                // restore state and start over
                indexedNode.index = startIndex;
                return findInsertionPosition(indexedNode, start, startCumLength, insertionSize, splitKey);
            }
            // adjust child length
            node.childLengths[index] += insertionSize;
            return findInsertionPosition(new IndexedNodeLinkedList(indexedNode, node.children[index], 0), start, cumulativeLength, insertionSize, splitKey);
        } else {
            // if it is not child, it's a key that might need to be split
            // if this is true, the position is right before the key
            if (cumulativeLength == start){
                // ensure that the rightmost position is chosen not only during potential splitting
                if (node.children[index].isFull()){
                    split(new IndexedNodeLinkedList(indexedNode, node.children[index],node.children[index].size-1));
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
                int leftIntervalLength = start-cumulativeLength;
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
                if (node.children[index + 1].isFull()){
                    // leftmost index because that is the goal
                    split(new IndexedNodeLinkedList(indexedNode, node.children[index + 1],0));
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
     *
     * @see IndexedNodeLinkedList
     */
    private void split(IndexedNodeLinkedList indexedNode) { // TODO: test
        // create node variable to operate on
        BtrfsNode node = indexedNode.node;
        // if the node to be split is the root, special stuff needs to happen
        if (node == this.root) { // split root if this is true
            // create new root node
            BtrfsNode newRoot = new BtrfsNode(this.degree);
            // the old root will be the left node
            BtrfsNode rightNode = new BtrfsNode(this.degree);
            // set up newRoot, starting with the key
            newRoot.keys[0] = node.keys[degree - 1];
            newRoot.size = 1;
            // the children
            newRoot.children[0] = node;
            newRoot.children[1] = rightNode;
            // set up children; the values are this way because this method is only called when the array to be split is full
            // fix children, their lengths and key arrays, start with the right node before modifying node since the values are taken from the left node
            System.arraycopy(node.keys, degree, rightNode.keys, 0, degree - 1);
            System.arraycopy(node.children, degree, rightNode.children, 0, degree);
            System.arraycopy(node.childLengths, degree, rightNode.childLengths, 0, degree);
            // maybe a bit hacky, but should work, the 2nd call is needed to restore the length by padding it; goal is to null all values that have been copied
            node.keys = Arrays.copyOf(Arrays.copyOf(node.keys, degree - 1), 2 * degree - 1);
            node.children = Arrays.copyOf(Arrays.copyOf(node.children, degree), 2 * degree);
            node.childLengths = Arrays.copyOf(Arrays.copyOf(node.childLengths, degree), 2 * degree);
            // fix children sizes
            node.size = degree - 1;
            rightNode.size = degree - 1;
            // once the children of the new root have been set up, calculate their lengths and store them
            newRoot.childLengths[0] = Arrays.stream(node.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(node.childLengths).sum();
            newRoot.childLengths[1] = Arrays.stream(rightNode.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(rightNode.childLengths).sum();
            // now, fix the IndexedNodeList; start by adding a new node to reference the new root
            indexedNode.parent = new IndexedNodeLinkedList(null, newRoot, indexedNode.index < degree ? 0 : 1);
            // check whether the indexedNode needs to be adjusted
            if (indexedNode.index >= degree) {
                indexedNode.node = rightNode;
                indexedNode.index -= degree;
            }
            // last of all, fix the root reference
            this.root = newRoot;
            return;
        }
        // before the splitting happens, the parent needs to be looked at; if the parent is full, split parent as well
        if (indexedNode.parent.node.isFull()) {
            // split the parent
            split(indexedNode.parent);
        }
        // now the parent will be able to hold the node when it is split
        BtrfsNode parent = indexedNode.parent.node;
        // new node for the right values
        BtrfsNode rightNode = new BtrfsNode(this.degree);
        // extract index in parent node
        int index = indexedNode.parent.index;
        // insert middle key of node in parent at the correct position; start by making room for the new key
        System.arraycopy(parent.keys, index, parent.keys, index + 1, parent.size - index);
        // insert the key
        parent.keys[index] = node.keys[degree - 1];
        // move children and their lengths
        System.arraycopy(parent.children, index + 1, parent.children, index + 2, parent.size - index);
        System.arraycopy(parent.childLengths, index + 1, parent.childLengths, index + 2, parent.size - index);
        // set the new child node
        parent.children[index + 1] = rightNode;
        // new node has been inserted, increase size
        parent.size++;
        // set up children; the values are this way because this method is only called when the array to be split is full
        // fix children, their lengths and key arrays, start with the right node before modifying node since the values are taken from the left node
        System.arraycopy(node.keys, degree, rightNode.keys, 0, degree - 1);
        System.arraycopy(node.children, degree, rightNode.children, 0, degree);
        System.arraycopy(node.childLengths, degree, rightNode.childLengths, 0, degree);
        // maybe a bit hacky, but should work, the 2nd call is needed to restore the length by padding it; goal is to null all values that have been copied
        node.keys = Arrays.copyOf(Arrays.copyOf(node.keys, degree - 1), 2 * degree - 1);
        node.children = Arrays.copyOf(Arrays.copyOf(node.children, degree), 2 * degree);
        node.childLengths = Arrays.copyOf(Arrays.copyOf(node.childLengths, degree), 2 * degree);
        // fix children sizes
        node.size = degree - 1;
        rightNode.size = degree - 1;
        // set the new values for the childLengths
        parent.childLengths[index] = Arrays.stream(node.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(node.childLengths).sum();
        parent.childLengths[index + 1] = Arrays.stream(rightNode.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(rightNode.childLengths).sum();
        // check whether the indexedNode needs to be adjusted
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

        throw new UnsupportedOperationException("Not implemented yet"); //TODO H3 d): remove if implemented
    }

    /**
     * Removes the leftmost key of the given node if it is a leaf.
     * Otherwise, it will remove the leftmost key of the first child.
     *
     * @param indexedNode the node to remove the leftmost key from.
     * @return the removed key.
     */
    private Interval removeLeftMostKey(IndexedNodeLinkedList indexedNode) {

        throw new UnsupportedOperationException("Not implemented yet"); //TODO H3 d): remove if implemented
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
        if (indexInParent + 1 < parent.children.length && parent.children[indexInParent + 1] != null && parent.children[indexInParent + 1].size >= degree) rotateFromRightSibling(indexedNode);
        // else, try left rotation
        else if (indexInParent - 1 >= 0 && parent.children[indexInParent - 1] != null && parent.children[indexInParent - 1].size >= degree) rotateFromLeftSibling(indexedNode);
        // if one of the rotations was possible, do a merge instead
        else{ // otherwise merge with one of the siblings since they are both at the lowest possible size
            // first, ensure that the parent is able to support the merge
            ensureSize(indexedNode.parent);
            // since things might have changed after the recursive call, reset local variables
            parent = indexedNode.parent.node;
            indexInParent = indexedNode.parent.index;
            // attempt right merge if possible
            if (indexInParent + 1 < parent.children.length && parent.children[indexInParent + 1] != null) mergeWithRightSibling(indexedNode);
            // else do a left merge because this is now guaranteed to be possible
            else mergeWithLeftSibling(indexedNode);
            // deal with root being empty
            if (root.size == 0){
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
        System.arraycopy(target.keys, 0, target.keys, leftSibling.size +1, target.size);
        System.arraycopy(target.children, 0, target.children, leftSibling.size +1, target.size +1);
        System.arraycopy(target.childLengths, 0, target.childLengths, leftSibling.size +1, target.size +1);
        // copy the key from parent
        target.keys[leftSibling.size] = parent.keys[indexInParent -1];
        // copy everything from the left sibling
        System.arraycopy(leftSibling.keys, 0, target.keys, 0, leftSibling.size);
        System.arraycopy(leftSibling.children, 0, target.children, 0, leftSibling.size +1);
        System.arraycopy(leftSibling.childLengths, 0, target.childLengths, 0, leftSibling.size +1);
        // adjust child length of target in parent
        parent.childLengths[indexInParent] = Arrays.stream(target.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(target.childLengths).sum();
        // shift everything in parent to account for the left sibling
        System.arraycopy(parent.keys, indexInParent, parent.keys, indexInParent - 1, parent.size-indexInParent);
        System.arraycopy(parent.children, indexInParent, parent.children, indexInParent - 1, parent.size-indexInParent +1);
        System.arraycopy(parent.childLengths, indexInParent, parent.childLengths, indexInParent - 1, parent.size-indexInParent+1);
        // adjust sizes
        parent.size--;
        target.size += leftSibling.size +1;
        // adjust indices
        indexedNode.parent.index--;
        indexedNode.index+=leftSibling.size +1;
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
        // TODO: why does this work here??? there is something wrong in 3a for sure
        parent.childLengths[indexInParent] = Arrays.stream(target.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(target.childLengths).sum();
        // parent.childLengths[indexInParent] += parent.keys[indexInParent].length() + rightSibling.keys[0].length() + rightSibling.childLengths[0] + rightSibling.childLengths[1];
        // move keys right of the index in parent to account for the removed key
        System.arraycopy(parent.keys, indexInParent + 1, parent.keys, indexInParent, parent.size-indexInParent);
        // also adjust children and child lengths
        System.arraycopy(parent.children, indexInParent + 2, parent.children, indexInParent +1, parent.size-indexInParent +1);
        System.arraycopy(parent.childLengths, indexInParent + 2, parent.childLengths, indexInParent+1, parent.size-indexInParent+1);
        // adjust sizes
        parent.size--;
        target.size += rightSibling.size;
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
        Interval leftKey = leftSibling.keys[leftSibling.size-1];
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
        // fix sizes
        target.size++;
        leftSibling.size--;
        // adjust child lengths
        // parent.childLengths[indexInParent] = Arrays.stream(target.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(target.childLengths).sum();
        // parent.childLengths[indexInParent -1] = Arrays.stream(leftSibling.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(leftSibling.childLengths).sum();
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
        // fix sizes
        target.size++;
        rightSibling.size--;
        // adjust child lengths
        // TODO: how is this wrong?? Could be a sign of a bigger issue!
        // parent.childLengths[indexInParent] = Arrays.stream(target.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(target.childLengths).sum();
        // parent.childLengths[indexInParent + 1] = Arrays.stream(rightSibling.keys).mapToInt(x -> x == null ? 0 : x.length()).sum() + Arrays.stream(rightSibling.childLengths).sum();
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
