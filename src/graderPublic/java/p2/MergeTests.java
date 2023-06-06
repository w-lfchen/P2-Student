package p2;

import org.junit.jupiter.params.ParameterizedTest;
import org.junitpioneer.jupiter.json.JsonClasspathSource;
import org.junitpioneer.jupiter.json.Property;
import org.sourcegrade.jagr.api.rubric.TestForSubmission;
import org.tudalgo.algoutils.tutor.general.assertions.Context;
import p2.btrfs.BtrfsFile;
import p2.btrfs.BtrfsNode;
import p2.btrfs.IndexedNodeLinkedList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertEquals;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.call;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.contextBuilder;
import static p2.TreeUtil.assertTreeEquals;
import static p2.TreeUtil.constructTree;
import static p2.TreeUtil.getRoot;
import static p2.TreeUtil.treeToString;

@SuppressWarnings("DuplicatedCode")
@TestForSubmission
public class MergeTests {

    @ParameterizedTest
    @JsonClasspathSource(value = "MergeTests.json", data = "testMergeRight")
    public void testMergeRightOriginalNode(@Property("tree") List<Object> tree,
                                           @Property("degree") int degree,
                                           @Property("parentIndex") int parentIndex,
                                           @Property("childIndex") int childIndex,
                                           @Property("expected") List<Object> expected) throws Throwable {

        testMerge(tree, degree, parentIndex, childIndex, parentIndex, childIndex, expected,
            file -> getRoot(file).children[parentIndex], true);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "MergeTests.json", data = "testMergeRight")
    public void testMergeRightParentNode(@Property("tree") List<Object> tree,
                                         @Property("degree") int degree,
                                         @Property("parentIndex") int parentIndex,
                                         @Property("childIndex") int childIndex,
                                         @Property("expected") List<Object> expected) throws Throwable {

        testMerge(tree, degree, parentIndex, childIndex, parentIndex, childIndex, expected,
            TreeUtil::getRoot, true);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "MergeTests.json", data = "testMergeLeft")
    public void testMergeLeftParentNode(@Property("tree") List<Object> tree,
                                        @Property("degree") int degree,
                                        @Property("parentIndex") int parentIndex,
                                        @Property("childIndex") int childIndex,
                                        @Property("expectedParentIndex") int expectedParentIndex,
                                        @Property("expectedChildIndex") int expectedChildIndex,
                                        @Property("expected") List<Object> expected) throws Throwable {

        testMerge(tree, degree, parentIndex, childIndex, expectedParentIndex, expectedChildIndex, expected,
            TreeUtil::getRoot, false);
    }

    private void testMerge(List<Object> tree,
                           int degree,
                           int parentIndex,
                           int childIndex,
                           int expectedParentIndex,
                           int expectedChildIndex,
                           List<Object> expected,
                           ThrowingFunction<BtrfsFile, BtrfsNode> testedNodeFunction,
                           boolean right) throws Throwable {

        Context.Builder<?> context = contextBuilder()
            .subject("BtrfsFile.mergeWith*Sibling()")
            .add("tree", treeToString(tree))
            .add("degree", degree)
            .add("parentIndex", parentIndex)
            .add("childIndex", childIndex)
            .add("expectedParentIndex", expectedParentIndex)
            .add("expectedChildIndex", expectedChildIndex)
            .add("expected tree", treeToString(expected));

        TreeUtil.FileAndStorage actualFileAndStorage = constructTree(tree, degree);
        BtrfsFile actualTree = actualFileAndStorage.file();
        BtrfsNode root = getRoot(actualTree);

        TreeUtil.FileAndStorage expectedFileAndStorage = constructTree(expected, degree);
        BtrfsFile expectedTree = expectedFileAndStorage.file();

        IndexedNodeLinkedList indexedRoot = new IndexedNodeLinkedList(null, root, parentIndex);
        IndexedNodeLinkedList indexedChild = new IndexedNodeLinkedList(indexedRoot, root.children[parentIndex], childIndex);

        if (right) {
            call(() -> callMergeRight(actualTree, indexedChild), context.build(),
                TR -> "mergeWithRightSibling() should not throw an exception");
        } else {
            call(() -> callMergeLeft(actualTree, indexedChild), context.build(),
                TR -> "mergeWithLeftSibling() should not throw an exception");
        }

        context.add("actual tree", treeToString(actualTree, actualFileAndStorage.storage()));

        assertTreeEquals(context.build(), "The tree is not correct.",
            testedNodeFunction.apply(expectedTree), testedNodeFunction.apply(actualTree),
            expectedFileAndStorage.storage(), actualFileAndStorage.storage());

        assertEquals(expectedTree.getSize(), actualTree.getSize(), context.build(),
            TR -> "The size of the tree should not change");

        assertEquals(expectedParentIndex, indexedRoot.index, context.build(),
            TR -> "The index of the parent should not change");
        assertEquals(root, indexedRoot.node, context.build(),
            TR -> "The node of the parent should not change");
        assertEquals(null, indexedRoot.parent, context.build(),
            TR -> "The parent of the parent should not change");

        assertEquals(expectedChildIndex, indexedChild.index, context.build(),
            TR -> "The index of the child is not correct");
        assertEquals(root.children[expectedParentIndex], indexedChild.node, context.build(),
            TR -> "The node of the child is not correct");
        assertEquals(indexedRoot, indexedChild.parent, context.build(),
            TR -> "The parent of the child should not change");
    }

    private void callMergeRight(BtrfsFile tree, IndexedNodeLinkedList indexedNode) throws Exception {

        Method method = BtrfsFile.class.getDeclaredMethod("mergeWithRightSibling", IndexedNodeLinkedList.class);
        method.setAccessible(true);
        try {
            method.invoke(tree, indexedNode);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            } else {
                throw e;
            }
        }
    }

    private void callMergeLeft(BtrfsFile tree, IndexedNodeLinkedList indexedNode) throws Exception {

        Method method = BtrfsFile.class.getDeclaredMethod("mergeWithLeftSibling", IndexedNodeLinkedList.class);
        method.setAccessible(true);
        try {
            method.invoke(tree, indexedNode);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            } else {
                throw e;
            }
        }
    }

}
