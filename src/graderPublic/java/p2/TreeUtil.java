package p2;

import org.tudalgo.algoutils.tutor.general.assertions.Context;
import p2.btrfs.BtrfsFile;
import p2.btrfs.BtrfsNode;
import p2.btrfs.IndexedNodeLinkedList;
import p2.storage.AllocationStrategy;
import p2.storage.FileSystem;
import p2.storage.Interval;
import p2.storage.Storage;
import p2.storage.StringEncoder;

import java.lang.reflect.Field;
import java.util.List;

import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertEquals;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertNotNull;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertNull;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertSame;

public class TreeUtil {

    public static void assertIntervalEquals(Context context,
                                            String expected,
                                            Interval actual,
                                            Storage actualStorage,
                                            String message) {


        byte[] actualBytes = new byte[actual.length()];
        actualStorage.read(actual.start(), actualBytes, 0, actual.length());

        String actualString = StringEncoder.INSTANCE.decode(actualBytes);

        assertEquals(expected, actualString, context, TR -> message.formatted(expected, actualString));
    }

    public static void assertIndexedNodeLinkedListEquals(Context context,
                                                         IndexedNodeLinkedList expected,
                                                         IndexedNodeLinkedList actual,
                                                         Storage storage) {
        assertIndexedNodeLinkedListEquals(context, expected, actual, storage, 0);
    }

    private static void assertIndexedNodeLinkedListEquals(Context context,
                                                         IndexedNodeLinkedList expected,
                                                         IndexedNodeLinkedList actual,
                                                         Storage storage,
                                                         int height) {

        assertSame(expected.node, actual.node, context,
            TR -> "The node of the IndexedNodeLinkedList at height %d is not correct. Expected Node: %s but was: %s"
                .formatted(height, treeToString(expected.node, storage), treeToString(actual.node, storage)));

        assertEquals(expected.index, actual.index, context,
            TR -> "The index of the IndexedNodeLinkedList at height %d is not correct. Expected Index: %s but was: %s"
                .formatted(height, expected.index, actual.index));

        if (expected.parent == null) {
            assertNull(actual.parent, context,
                TR -> ("The parent of the IndexedNodeLinkedList at height %d is not correct. " +
                    "Expected the parent to be null but it was: %s")
                    .formatted(height, treeToString(actual.parent.node, storage)));
            return;
        }

        assertIndexedNodeLinkedListEquals(context, expected.parent, actual.parent, storage, height + 1);
    }

    public static void assertTreeEquals(Context context, String message, BtrfsNode expectedNode, BtrfsNode actualNode,
                                        Storage expectedStorage, Storage actualStorage) {

        if (expectedNode == null) {
            assertNull(actualNode, context,
                TR -> message + " The node is not null. Expected Node: null");
            return;
        } else {
            assertNotNull(actualNode, context,
                TR -> message + " The node is null. Expected Node: %s".formatted(treeToString(expectedNode, expectedStorage)));
        }

        assertEquals(expectedNode.size, actualNode.size, context,
            TR -> message + " The size of the node %s is not correct. Expected Node: %s"
                .formatted(treeToString(actualNode, actualStorage), treeToString(expectedNode, expectedStorage)));

        for (int i = 0; i < expectedNode.size; i++) {
            int finalI = i;

            byte[] expectedBytes = new byte[expectedNode.keys[i].length()];
            expectedStorage.read(expectedNode.keys[i].start(), expectedBytes, 0, expectedNode.keys[i].length());
            String expectedString = StringEncoder.INSTANCE.decode(expectedBytes);

            assertNotNull(actualNode.keys[i], context, TR -> message + " The key at index %d of the node %s should not be <null>"
                .formatted(finalI, treeToString(actualNode, actualStorage)));

            byte[] actualBytes = new byte[actualNode.keys[i].length()];
            actualStorage.read(actualNode.keys[i].start(), actualBytes, 0, actualNode.keys[i].length());
            String actualString = StringEncoder.INSTANCE.decode(actualBytes);

            assertEquals(expectedString, actualString, context,
                TR -> message + " The key at index %d of the node %s is not correct. Expected Node: %s"
                    .formatted(finalI, treeToString(actualNode, actualStorage), treeToString(expectedNode, expectedStorage)));
        }

        for (int i = 0; i < actualNode.size + 1; i++) {
            int finalI = i;

            if (expectedNode.children[i] != null) {
                assertNotNull(actualNode.children[i], context,
                    TR -> message + " The child at index %d of the node %s is null. Expected Node: %s"
                        .formatted(finalI, treeToString(actualNode, actualStorage), treeToString(expectedNode, expectedStorage)));
                assertTreeEquals(context, message, expectedNode.children[i], actualNode.children[i], expectedStorage, actualStorage);
            } else {
                assertNull(actualNode.children[i], context,
                    TR -> message + " The child at index %d of the node %s is not null. Expected Node: %s"
                        .formatted(finalI, treeToString(actualNode, actualStorage), treeToString(expectedNode, expectedStorage)));
            }

            assertEquals(expectedNode.childLengths[i], actualNode.childLengths[i], context,
                TR -> message + " The childLength at index %d of the node %s is not correct. Expected Node: %s"
                    .formatted(finalI, treeToString(actualNode, actualStorage), treeToString(expectedNode, expectedStorage)));
        }
    }

    public static String treeToString(BtrfsFile file, Storage storage) throws NoSuchFieldException, IllegalAccessException {
        return treeToString(getRoot(file), storage);
    }

    public static String treeToString(BtrfsNode node, Storage storage) {
        StringBuilder builder = new StringBuilder();
        try {
            treeToString(node, storage, builder);
        } catch (Exception e) {
            return "<failed to convert tree to string>";
        }
        return builder.toString();
    }

    private static void treeToString(BtrfsNode node, Storage storage, StringBuilder builder) {
        builder.append("{");
        for (int i = 0; i < node.size; i++) {
            if (node.children[i] != null) {
                if (i > 0) {
                    builder.append(",");
                }

                treeToString(node.children[i], storage, builder);
            }

            if (i > 0 || node.children[0] != null) {
                builder.append(",");
            }

            if (node.keys[i] != null) {
                builder.append("\"").append(keyToString(storage, node.keys[i])).append("\"");
            } else {
                builder.append("<null>");
            }
        }

        if (node.children[node.size] != null) {
            if (node.size > 0) {
                builder.append(",");
            }
            treeToString(node.children[node.size], storage, builder);
        }

        builder.append("}");
    }

    public static String treeToString(List<Object> tree) {
        StringBuilder builder = new StringBuilder();
        try {
            treeToString(tree, builder);
        } catch (Exception e) {
            return "<failed to convert tree to string>";
        }
        return builder.toString();
    }

    @SuppressWarnings("unchecked")
    private static void treeToString(List<Object> tree, StringBuilder builder) {
        builder.append("{");
        for (int i = 0; i < tree.size(); i++) {
            Object current = tree.get(i);

            if (current == null) {
                continue;
            }

            if (current instanceof List) {
                treeToString((List<Object>) current, builder);
            } else {
                builder.append("\"").append(current).append("\"");
            }
            if (i < tree.size() - 1) {
                builder.append(",");
            }
        }
        builder.append("}");
    }

    public static String keyToString(Storage storage, Interval key) {

        byte[] data = new byte[key.length()];
        storage.read(key.start(), data, 0, key.length());
        return StringEncoder.INSTANCE.decode(data);
    }

    public static FileAndStorage constructTree(List<Object> tree, int degree) throws NoSuchFieldException, IllegalAccessException {

        FileSystem fileSystem = new FileSystem(AllocationStrategy.NEXT_FIT, 100);

        Storage storage = getStorage(fileSystem);
        AllocationStrategy allocator = getAllocator(fileSystem);
        allocator.setMaxIntervalSize(Integer.MAX_VALUE);

        NodeAndLength root = constructRoot(tree, degree, storage, allocator);
        BtrfsFile file = new BtrfsFile("test.txt", storage, degree);
        setRoot(file, root.node);
        setSize(file, root.length);

        return new FileAndStorage(file, storage);
    }

    public record FileAndStorage(BtrfsFile file, Storage storage) {
    }

    public static AllocationStrategy getAllocator(FileSystem fileSystem) throws NoSuchFieldException, IllegalAccessException {
        Field allocatorField = FileSystem.class.getDeclaredField("allocator");
        allocatorField.setAccessible(true);
        return (AllocationStrategy) allocatorField.get(fileSystem);
    }

    public static Storage getStorage(FileSystem fileSystem) throws NoSuchFieldException, IllegalAccessException {
        Field storageField = FileSystem.class.getDeclaredField("storage");
        storageField.setAccessible(true);
        return (Storage) storageField.get(fileSystem);
    }

    @SuppressWarnings("unchecked")
    public static NodeAndLength constructRoot(List<Object> tree, int degree, Storage storage, AllocationStrategy allocator) {


        int length = 0;
        BtrfsNode root = new BtrfsNode(degree);

        for (Object current : tree) {
            if (current instanceof List) {
                NodeAndLength child = constructRoot((List<Object>) current, degree, storage, allocator);
                root.children[root.size] = child.node;
                root.childLengths[root.size] = child.length;
                length += child.length;
            } else {

                String string = (String) current;

                byte[] data = StringEncoder.INSTANCE.encode(string);

                List<Interval> intervals = allocator.allocate(data.length);

                if (intervals.size() > 1) {
                    throw new IllegalStateException("internal Error: allocator should not return more than one interval");
                }

                storage.write(intervals.get(0).start(), data, 0, data.length);

                root.keys[root.size] = intervals.get(0);
                root.size++;
                length += data.length;
            }
        }


        return new NodeAndLength(root, length);
    }

    private record NodeAndLength(BtrfsNode node, int length) {
    }

    public static BtrfsNode getRoot(BtrfsFile file) throws NoSuchFieldException, IllegalAccessException {
        Field field = BtrfsFile.class.getDeclaredField("root");
        field.setAccessible(true);
        return (BtrfsNode) field.get(file);
    }

    public static void setRoot(BtrfsFile file, BtrfsNode root) throws NoSuchFieldException, IllegalAccessException {
        Field field = BtrfsFile.class.getDeclaredField("root");
        field.setAccessible(true);
        field.set(file, root);
    }

    public static void setSize(BtrfsFile file, int size) throws NoSuchFieldException, IllegalAccessException {
        Field field = BtrfsFile.class.getDeclaredField("size");
        field.setAccessible(true);
        field.set(file, size);
    }
}
